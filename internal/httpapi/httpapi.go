package httpapi

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"runtime"
	"time"

	"github.com/aababababababbabaabababababba/headless-rtmp/internal/config"
	"github.com/aababababababbabaabababababba/headless-rtmp/internal/hub"
)

type Server struct {
	cfg *config.Config
	hub *hub.Hub
	log *log.Logger
	srv *http.Server
}

func New(cfg *config.Config, h *hub.Hub, logger *log.Logger) *Server {
	return &Server{cfg: cfg, hub: h, log: logger}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/stats", s.stats)
	mux.HandleFunc("/streams", s.streams)

	s.srv = &http.Server{
		Addr:              s.cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	s.log.Printf("http listening on %s", s.cfg.HTTPAddr)
	err := s.srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *Server) Shutdown(ctx context.Context) {
	if s.srv != nil {
		_ = s.srv.Shutdown(ctx)
	}
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) stats(w http.ResponseWriter, _ *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	out := map[string]interface{}{
		"goroutines": runtime.NumGoroutine(),
		"heap_alloc": ms.HeapAlloc,
		"heap_sys":   ms.HeapSys,
		"num_gc":     ms.NumGC,
		"streams":    s.hub.Stats(),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) streams(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.hub.Stats())
}
