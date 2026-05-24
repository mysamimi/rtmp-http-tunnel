package server

import (
	"context"
	"log"
	"rtmp-http-tunnel/pkg/common"
)

type Server struct {
	cfg *common.ServerConfig
}

func NewServer(cfg *common.ServerConfig) *Server {
	return &Server{
		cfg: cfg,
	}
}

// Start launches the Server components (Receiver + Streamer)
func (s *Server) Start(ctx context.Context) error {
	buffer := NewBuffer(s.cfg)
	receiver := NewReceiver(s.cfg, buffer)
	streamer := NewStreamer(s.cfg, buffer)

	// Run streamer in a separate goroutine
	go func() {
		if err := streamer.Start(ctx); err != nil && err != context.Canceled {
			log.Printf("[Server] Streamer exited with error: %v", err)
		}
	}()

	log.Println("[Server] Server orchestrator started successfully")

	// Start HTTP/HTTPS receiver (blocks until server stops or error occurs)
	return receiver.Start()
}
