package hub

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type PacketType uint8

const (
	PktAudio PacketType = 8
	PktVideo PacketType = 9
	PktMeta  PacketType = 18
)

type Packet struct {
	Type      PacketType
	Timestamp uint32
	Data      []byte
	IsKey     bool
	IsSeqHdr  bool
}

type Stream struct {
	Path        string
	StartedAt   time.Time
	publisher   atomic.Pointer[publisher]
	mu          sync.RWMutex
	players     map[*Player]struct{}
	bytesIn     atomic.Uint64
	bytesOut    atomic.Uint64
	framesIn    atomic.Uint64
	audioSeqHdr atomic.Pointer[Packet]
	videoSeqHdr atomic.Pointer[Packet]
	metadata    atomic.Pointer[Packet]
	gopCache    []*Packet
	gopMu       sync.Mutex
	keepGOP     bool
}

type publisher struct {
	remote string
	since  time.Time
}

type Player struct {
	Path    string
	Remote  string
	Since   time.Time
	Out     chan *Packet
	dropped atomic.Uint64
	closed  atomic.Bool
}

func (p *Player) Dropped() uint64 { return p.dropped.Load() }

type Hub struct {
	mu      sync.RWMutex
	streams map[string]*Stream
	log     *log.Logger
}

func New(logger *log.Logger) *Hub {
	return &Hub{streams: map[string]*Stream{}, log: logger}
}

func (h *Hub) get(path string) *Stream {
	h.mu.RLock()
	s := h.streams[path]
	h.mu.RUnlock()
	return s
}

func (h *Hub) getOrCreate(path string, keepGOP bool) *Stream {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.streams[path]; ok {
		return s
	}
	s := &Stream{
		Path:      path,
		StartedAt: time.Now(),
		players:   map[*Player]struct{}{},
		keepGOP:   keepGOP,
	}
	h.streams[path] = s
	return s
}

var ErrAlreadyPublishing = errors.New("stream already has a publisher")

func (h *Hub) AttachPublisher(path, remote string, keepGOP bool) (*Stream, error) {
	s := h.getOrCreate(path, keepGOP)
	if !s.publisher.CompareAndSwap(nil, &publisher{remote: remote, since: time.Now()}) {
		return nil, ErrAlreadyPublishing
	}
	h.log.Printf("publisher attached path=%s remote=%s", path, remote)
	return s, nil
}

func (h *Hub) DetachPublisher(s *Stream) {
	if s == nil {
		return
	}
	s.publisher.Store(nil)
	s.gopMu.Lock()
	s.gopCache = nil
	s.gopMu.Unlock()
	s.audioSeqHdr.Store(nil)
	s.videoSeqHdr.Store(nil)
	s.metadata.Store(nil)
	h.mu.Lock()
	defer h.mu.Unlock()
	s.mu.Lock()
	for p := range s.players {
		p.closed.Store(true)
		close(p.Out)
	}
	s.players = map[*Player]struct{}{}
	s.mu.Unlock()
	delete(h.streams, s.Path)
	h.log.Printf("publisher detached path=%s", s.Path)
}

func (h *Hub) AttachPlayer(path, remote string, buf int) (*Stream, *Player, error) {
	s := h.get(path)
	if s == nil || s.publisher.Load() == nil {
		return nil, nil, errors.New("no publisher for " + path)
	}
	p := &Player{
		Path:   path,
		Remote: remote,
		Since:  time.Now(),
		Out:    make(chan *Packet, buf),
	}
	s.mu.Lock()
	s.players[p] = struct{}{}
	s.mu.Unlock()

	if m := s.metadata.Load(); m != nil {
		p.Out <- m
	}
	if v := s.videoSeqHdr.Load(); v != nil {
		p.Out <- v
	}
	if a := s.audioSeqHdr.Load(); a != nil {
		p.Out <- a
	}
	if s.keepGOP {
		s.gopMu.Lock()
		cache := make([]*Packet, len(s.gopCache))
		copy(cache, s.gopCache)
		s.gopMu.Unlock()
		for _, pkt := range cache {
			select {
			case p.Out <- pkt:
			default:
			}
		}
	}

	h.log.Printf("player attached path=%s remote=%s players=%d", path, remote, h.PlayerCount(s))
	return s, p, nil
}

func (h *Hub) DetachPlayer(s *Stream, p *Player) {
	if s == nil || p == nil {
		return
	}
	s.mu.Lock()
	if _, ok := s.players[p]; ok {
		delete(s.players, p)
		if !p.closed.Swap(true) {
			close(p.Out)
		}
	}
	s.mu.Unlock()
	h.log.Printf("player detached path=%s remote=%s dropped=%d", s.Path, p.Remote, p.Dropped())
}

func (h *Hub) Publish(s *Stream, pkt *Packet) {
	s.bytesIn.Add(uint64(len(pkt.Data)))
	s.framesIn.Add(1)

	switch pkt.Type {
	case PktMeta:
		cp := clonePkt(pkt)
		s.metadata.Store(cp)
	case PktAudio:
		if pkt.IsSeqHdr {
			cp := clonePkt(pkt)
			s.audioSeqHdr.Store(cp)
		}
	case PktVideo:
		if pkt.IsSeqHdr {
			cp := clonePkt(pkt)
			s.videoSeqHdr.Store(cp)
		}
	}

	if s.keepGOP && (pkt.Type == PktAudio || pkt.Type == PktVideo) && !pkt.IsSeqHdr {
		s.gopMu.Lock()
		if pkt.Type == PktVideo && pkt.IsKey {
			s.gopCache = s.gopCache[:0]
		}
		if len(s.gopCache) < 1024 {
			s.gopCache = append(s.gopCache, pkt)
		}
		s.gopMu.Unlock()
	}

	s.mu.RLock()
	for p := range s.players {
		if p.closed.Load() {
			continue
		}
		select {
		case p.Out <- pkt:
			s.bytesOut.Add(uint64(len(pkt.Data)))
		default:
			p.dropped.Add(1)
		}
	}
	s.mu.RUnlock()
}

func clonePkt(p *Packet) *Packet {
	cp := *p
	cp.Data = append([]byte(nil), p.Data...)
	return &cp
}

func (h *Hub) PlayerCount(s *Stream) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.players)
}

type StreamStat struct {
	Path      string    `json:"path"`
	StartedAt time.Time `json:"started_at"`
	Publisher string    `json:"publisher"`
	Players   int       `json:"players"`
	BytesIn   uint64    `json:"bytes_in"`
	BytesOut  uint64    `json:"bytes_out"`
	FramesIn  uint64    `json:"frames_in"`
}

func (h *Hub) Stats() []StreamStat {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]StreamStat, 0, len(h.streams))
	for _, s := range h.streams {
		pub := ""
		if p := s.publisher.Load(); p != nil {
			pub = p.remote
		}
		out = append(out, StreamStat{
			Path:      s.Path,
			StartedAt: s.StartedAt,
			Publisher: pub,
			Players:   h.PlayerCount(s),
			BytesIn:   s.bytesIn.Load(),
			BytesOut:  s.bytesOut.Load(),
			FramesIn:  s.framesIn.Load(),
		})
	}
	return out
}
