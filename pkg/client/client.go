package client

import (
	"context"
	"log"
	"rtmp-http-tunnel/pkg/common"
)

type Client struct {
	cfg *common.ClientConfig
}

func NewClient(cfg *common.ClientConfig) *Client {
	return &Client{
		cfg: cfg,
	}
}

// Start starts the entire client pipeline (Ingester + Uploader)
func (c *Client) Start(ctx context.Context) error {
	// Create a channel for passing chunk jobs from Ingester to Uploader
	// Buffered to hold up to 100 chunks in queue if network goes slow
	chunkChan := make(chan ChunkJob, 100)

	ingester := NewIngester(c.cfg, chunkChan)
	uploader := NewUploader(c.cfg, chunkChan)

	// Run uploader in a separate goroutine
	go func() {
		uploader.Start(ctx)
	}()

	log.Println("[Client] Client orchestrator started successfully")
	
	// Start ingester (blocks until context is cancelled or fatal error occurs)
	err := ingester.Start(ctx)
	
	// Close chunk channel so uploader workers exit
	close(chunkChan)
	
	return err
}
