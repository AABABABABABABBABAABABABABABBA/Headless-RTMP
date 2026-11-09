package rtmp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/aababababababbabaabababababba/headless-rtmp/internal/config"
	"github.com/aababababababbabaabababababba/headless-rtmp/internal/hub"
)

const (
	msgSetChunkSize   = 1
	msgAbort          = 2
	msgAck            = 3
	msgUserControl    = 4
	msgWindowAckSize  = 5
	msgSetPeerBW      = 6
	msgAudio          = 8
	msgVideo          = 9
	msgAMF3Data       = 15
	msgAMF3Command    = 17
	msgAMF0Data       = 18
	msgAMF0Command    = 20
)

const (
	csidProtocolControl = 2
	csidCommand         = 3
	csidAudio           = 4
	csidVideo           = 5
	csidData            = 6
)

type conn struct {
	c       net.Conn
	cfg     *config.Config
	hub     *hub.Hub
	log     *log.Logger
	cr      *chunkReader
	cw      *chunkWriter
	writeMu sync.Mutex

	app        string
	streamID   uint32
	isPublish  bool
	isPlay     bool
	streamPath string
	streamKey  string

	pubStream  *hub.Stream
	player     *hub.Player
}

func newConn(c net.Conn, cfg *config.Config, h *hub.Hub, logger *log.Logger) *conn {
	return &conn{c: c, cfg: cfg, hub: h, log: logger, streamID: 1}
}

func (co *conn) serve(ctx context.Context) {
	defer co.c.Close()
	remote := co.c.RemoteAddr().String()
	co.log.Printf("conn open remote=%s", remote)
	defer func() {
		if co.isPublish && co.pubStream != nil {
			co.hub.DetachPublisher(co.pubStream)
		}
		if co.isPlay && co.player != nil {
			co.hub.DetachPlayer(co.pubStream, co.player)
		}
		co.log.Printf("conn close remote=%s", remote)
	}()

	co.c.SetDeadline(time.Now().Add(time.Duration(co.cfg.ReadTimeoutS) * time.Second))
	if err := serverHandshake(co.c); err != nil {
		co.log.Printf("handshake err remote=%s: %v", remote, err)
		return
	}

	co.cr = newChunkReader(co.c)
	co.cw = newChunkWriter(co.c)

	go func() {
		<-ctx.Done()
		co.c.Close()
	}()

	for {
		co.c.SetReadDeadline(time.Now().Add(time.Duration(co.cfg.ReadTimeoutS) * time.Second))
		m, err := co.cr.readMessage()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				co.log.Printf("read err remote=%s: %v", remote, err)
			}
			return
		}
		if err := co.handleMessage(m); err != nil {
			co.log.Printf("handle err remote=%s type=%d: %v", remote, m.typeID, err)
			return
		}
	}
}

func (co *conn) handleMessage(m *message) error {
	switch m.typeID {
	case msgSetChunkSize:
		if len(m.payload) < 4 {
			return errors.New("short set chunk size")
		}
		n := binary.BigEndian.Uint32(m.payload[:4]) & 0x7FFFFFFF
		co.cr.setChunkSize(n)
		return nil
	case msgAbort, msgAck, msgWindowAckSize, msgSetPeerBW, msgUserControl:
		return nil
	case msgAMF0Command:
		return co.handleCommand(m)
	case msgAMF0Data, msgAMF3Data:
		return co.handleData(m)
	case msgAudio:
		return co.ingest(m, hub.PktAudio)
	case msgVideo:
		return co.ingest(m, hub.PktVideo)
	case msgAMF3Command:
		if len(m.payload) > 0 {
			nm := *m
			nm.payload = m.payload[1:]
			return co.handleCommand(&nm)
		}
	}
	return nil
}

func (co *conn) ingest(m *message, t hub.PacketType) error {
	if !co.isPublish || co.pubStream == nil {
		return nil
	}
	p := &hub.Packet{
		Type:      t,
		Timestamp: m.timestamp,
		Data:      append([]byte(nil), m.payload...),
	}
	if t == hub.PktVideo && len(m.payload) >= 2 {
		frameType := m.payload[0] >> 4
		codecID := m.payload[0] & 0x0F
		p.IsKey = frameType == 1
		if codecID == 7 && m.payload[1] == 0 {
			p.IsSeqHdr = true
		}
	}
	if t == hub.PktAudio && len(m.payload) >= 2 {
		soundFormat := m.payload[0] >> 4
		if soundFormat == 10 && m.payload[1] == 0 {
			p.IsSeqHdr = true
		}
	}
	co.hub.Publish(co.pubStream, p)
	return nil
}

func (co *conn) handleData(m *message) error {
	if !co.isPublish || co.pubStream == nil {
		return nil
	}
	p := &hub.Packet{
		Type:      hub.PktMeta,
		Timestamp: m.timestamp,
		Data:      append([]byte(nil), m.payload...),
	}
	co.hub.Publish(co.pubStream, p)
	return nil
}

func (co *conn) handleCommand(m *message) error {
	vals, err := decodeAMF(m.payload)
	if err != nil {
		return err
	}
	if len(vals) < 2 {
		return errors.New("short command")
	}
	name, _ := strOf(vals[0])
	txID, _ := numOf(vals[1])

	switch name {
	case "connect":
		return co.onConnect(txID, vals)
	case "releaseStream", "FCPublish", "FCUnpublish", "FCSubscribe":
		return co.respondVoid(txID)
	case "createStream":
		return co.onCreateStream(txID)
	case "publish":
		return co.onPublish(txID, vals, m.streamID)
	case "play":
		return co.onPlay(txID, vals, m.streamID)
	case "deleteStream", "closeStream":
		return nil
	case "getStreamLength":
		return co.respondVoid(txID)
	}
	return nil
}

func (co *conn) onConnect(txID float64, vals []interface{}) error {
	if len(vals) >= 3 {
		if obj, ok := vals[2].(amfObjectMap); ok {
			if v, ok := obj["app"].(string); ok {
				co.app = v
			}
		}
	}
	if co.app != co.cfg.App {
		return fmt.Errorf("unknown app %q (expected %q)", co.app, co.cfg.App)
	}

	if err := co.writeWindowAckSize(defaultWindowAckSz); err != nil {
		return err
	}
	if err := co.writeSetPeerBW(defaultPeerBW, 2); err != nil {
		return err
	}
	if err := co.writeUserControlStreamBegin(0); err != nil {
		return err
	}
	if err := co.writeSetChunkSize(uint32(co.cfg.ChunkSize)); err != nil {
		return err
	}
	co.cw.setChunkSize(uint32(co.cfg.ChunkSize))

	props := amfObjectMap{
		"fmsVer":       "FMS/3,0,1,123",
		"capabilities": 31.0,
	}
	info := amfObjectMap{
		"level":          "status",
		"code":           "NetConnection.Connect.Success",
		"description":    "Connection succeeded.",
		"objectEncoding": 0.0,
	}
	payload := encodeAMFCommand("_result", txID, props, info)
	return co.writeCommand(payload)
}

func (co *conn) onCreateStream(txID float64) error {
	payload := encodeAMFCommand("_result", txID, nil, float64(co.streamID))
	return co.writeCommand(payload)
}

func (co *conn) onPublish(txID float64, vals []interface{}, _ uint32) error {
	if len(vals) < 4 {
		return errors.New("publish: missing name")
	}
	name, _ := strOf(vals[3])
	if name == "" {
		return errors.New("publish: empty name")
	}
	key := ""
	if i := strings.IndexByte(name, '?'); i >= 0 {
		q := name[i+1:]
		name = name[:i]
		for _, kv := range strings.Split(q, "&") {
			if strings.HasPrefix(kv, "key=") {
				key = kv[4:]
			}
		}
	}
	co.streamKey = key
	if co.cfg.PublishKey != "" && key != co.cfg.PublishKey {
		return co.status("error", "NetStream.Publish.BadAuth", "bad publish key")
	}
	path := co.app + "/" + name
	s, err := co.hub.AttachPublisher(path, co.c.RemoteAddr().String(), co.cfg.GOPCache)
	if err != nil {
		return co.status("error", "NetStream.Publish.BadName", err.Error())
	}
	co.pubStream = s
	co.streamPath = path
	co.isPublish = true
	if err := co.writeUserControlStreamBegin(co.streamID); err != nil {
		return err
	}
	return co.status("status", "NetStream.Publish.Start", "Start publishing")
}

func (co *conn) onPlay(txID float64, vals []interface{}, _ uint32) error {
	if len(vals) < 4 {
		return errors.New("play: missing name")
	}
	name, _ := strOf(vals[3])
	if name == "" {
		return errors.New("play: empty name")
	}
	if i := strings.IndexByte(name, '?'); i >= 0 {
		name = name[:i]
	}
	path := co.app + "/" + name
	if err := co.writeUserControlStreamBegin(co.streamID); err != nil {
		return err
	}
	if err := co.status("status", "NetStream.Play.Reset", "Playing and resetting"); err != nil {
		return err
	}
	if err := co.status("status", "NetStream.Play.Start", "Started playing"); err != nil {
		return err
	}
	s, p, err := co.hub.AttachPlayer(path, co.c.RemoteAddr().String(), 1024)
	if err != nil {
		return co.status("error", "NetStream.Play.StreamNotFound", err.Error())
	}
	co.pubStream = s
	co.player = p
	co.isPlay = true
	go co.pumpPlayer()
	return nil
}

func (co *conn) pumpPlayer() {
	defer co.c.Close()
	for pkt := range co.player.Out {
		co.c.SetWriteDeadline(time.Now().Add(time.Duration(co.cfg.WriteTimeoutS) * time.Second))
		var csid uint32
		switch pkt.Type {
		case hub.PktAudio:
			csid = csidAudio
		case hub.PktVideo:
			csid = csidVideo
		case hub.PktMeta:
			csid = csidData
		}
		co.writeMu.Lock()
		err := co.cw.writeMessage(csid, uint8(pkt.Type), co.streamID, pkt.Timestamp, pkt.Data)
		if err == nil {
			err = co.cw.flush()
		}
		co.writeMu.Unlock()
		if err != nil {
			return
		}
	}
}

func (co *conn) respondVoid(txID float64) error {
	return co.writeCommand(encodeAMFCommand("_result", txID, nil, nil))
}

func (co *conn) writeCommand(payload []byte) error {
	co.writeMu.Lock()
	defer co.writeMu.Unlock()
	if err := co.cw.writeMessage(csidCommand, msgAMF0Command, 0, 0, payload); err != nil {
		return err
	}
	return co.cw.flush()
}

func (co *conn) status(level, code, desc string) error {
	info := amfObjectMap{
		"level":       level,
		"code":        code,
		"description": desc,
	}
	payload := encodeAMFCommand("onStatus", 0, nil, info)
	co.writeMu.Lock()
	defer co.writeMu.Unlock()
	if err := co.cw.writeMessage(csidCommand, msgAMF0Command, co.streamID, 0, payload); err != nil {
		return err
	}
	return co.cw.flush()
}

func (co *conn) writeWindowAckSize(n uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], n)
	co.writeMu.Lock()
	defer co.writeMu.Unlock()
	if err := co.cw.writeMessage(csidProtocolControl, msgWindowAckSize, 0, 0, b[:]); err != nil {
		return err
	}
	return co.cw.flush()
}

func (co *conn) writeSetPeerBW(n uint32, limit byte) error {
	var b [5]byte
	binary.BigEndian.PutUint32(b[:4], n)
	b[4] = limit
	co.writeMu.Lock()
	defer co.writeMu.Unlock()
	if err := co.cw.writeMessage(csidProtocolControl, msgSetPeerBW, 0, 0, b[:]); err != nil {
		return err
	}
	return co.cw.flush()
}

func (co *conn) writeSetChunkSize(n uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], n&0x7FFFFFFF)
	co.writeMu.Lock()
	defer co.writeMu.Unlock()
	if err := co.cw.writeMessage(csidProtocolControl, msgSetChunkSize, 0, 0, b[:]); err != nil {
		return err
	}
	return co.cw.flush()
}

func (co *conn) writeUserControlStreamBegin(streamID uint32) error {
	var b [6]byte
	binary.BigEndian.PutUint16(b[0:2], 0)
	binary.BigEndian.PutUint32(b[2:6], streamID)
	co.writeMu.Lock()
	defer co.writeMu.Unlock()
	if err := co.cw.writeMessage(csidProtocolControl, msgUserControl, 0, 0, b[:]); err != nil {
		return err
	}
	return co.cw.flush()
}
