package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aababababababbabaabababababba/headless-rtmp/internal/config"
	"github.com/aababababababbabaabababababba/headless-rtmp/internal/hub"
	"github.com/aababababababbabaabababababba/headless-rtmp/internal/httpapi"
	"github.com/aababababababbabaabababababba/headless-rtmp/internal/rtmp"
)

func main() {
	cfgPath := flag.String("config", "", "optional path to config file (env still wins)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	logger.Printf("headless-rtmp starting: rtmp=%s http=%s app=%s", cfg.RTMPAddr, cfg.HTTPAddr, cfg.App)

	h := hub.New(logger)

	rtmpSrv := rtmp.NewServer(cfg, h, logger)
	httpSrv := httpapi.New(cfg, h, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := rtmpSrv.ListenAndServe(ctx); err != nil {
			logger.Printf("rtmp server exited: %v", err)
			stop()
		}
	}()

	go func() {
		if err := httpSrv.ListenAndServe(ctx); err != nil {
			logger.Printf("http server exited: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Printf("shutdown signal received, draining...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rtmpSrv.Shutdown(shutdownCtx)
	httpSrv.Shutdown(shutdownCtx)
	logger.Printf("bye")
}
