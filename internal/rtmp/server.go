package rtmp

import (
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aababababababbabaabababababba/headless-rtmp/internal/config"
	"github.com/aababababababbabaabababababba/headless-rtmp/internal/hub"
)

type Server struct {
	cfg *config.Config
	hub *hub.Hub
	log *log.Logger

	lnMu sync.Mutex
	ln   net.Listener

	connCount atomic.Int64
	wg        sync.WaitGroup
}

func NewServer(cfg *config.Config, h *hub.Hub, logger *log.Logger) *Server {
	return &Server{cfg: cfg, hub: h, log: logger}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	lc := net.ListenConfig{KeepAlive: 30 * time.Second}
	ln, err := lc.Listen(ctx, "tcp", s.cfg.RTMPAddr)
	if err != nil {
		return err
	}
	s.lnMu.Lock()
	s.ln = ln
	s.lnMu.Unlock()
	s.log.Printf("rtmp listening on %s", s.cfg.RTMPAddr)

	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return err
		}
		if tcp, ok := c.(*net.TCPConn); ok {
			tcp.SetNoDelay(true)
			tcp.SetKeepAlive(true)
			tcp.SetKeepAlivePeriod(30 * time.Second)
		}
		s.connCount.Add(1)
		s.wg.Add(1)
		co := newConn(c, s.cfg, s.hub, s.log)
		go func() {
			defer s.wg.Done()
			defer s.connCount.Add(-1)
			co.serve(ctx)
		}()
	}
}

func (s *Server) Shutdown(ctx context.Context) {
	s.lnMu.Lock()
	ln := s.ln
	s.lnMu.Unlock()
	if ln != nil {
		ln.Close()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (s *Server) ActiveConns() int64 { return s.connCount.Load() }
