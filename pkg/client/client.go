package client

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

type Client struct {
	cfg *common.ClientConfig
}

func NewClient(cfg *common.ClientConfig) *Client {
	return &Client{
		cfg: cfg,
	}
}

// Start starts the entire client pipeline (orchestrates one goroutine per stream)
func (c *Client) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, stream := range c.cfg.Streams {
		wg.Add(1)
		go func(sc common.StreamConfig) {
			defer wg.Done()
			runStream(ctx, c.cfg, sc)
		}(stream)
	}
	wg.Wait()
	return nil
}

func runStream(ctx context.Context, cfg *common.ClientConfig, sc common.StreamConfig) {
	log.Printf("[Client] Starting stream pipeline for path: %s (Input: %s, Temp: %s)", sc.StreamPath, sc.InputURL, sc.TempDir)

	// Ensure local temp dir exists
	if err := os.MkdirAll(sc.TempDir, 0755); err != nil {
		log.Printf("[Client] [Stream %s] Failed to create temp dir %s: %v", sc.StreamPath, sc.TempDir, err)
		return
	}

	// Stale chunks are removed by the Ingester at startup; the upload sequence
	// state file in this directory must survive client restarts.

	// Channel for passing chunk jobs from Ingester to Uploader for this specific stream
	chunkChan := make(chan ChunkJob, 100)

	ingester := NewIngester(cfg, sc, chunkChan)
	uploader := NewUploader(cfg, sc, chunkChan)

	// Run uploader in a separate goroutine
	uploaderCtx, uploaderCancel := context.WithCancel(ctx)
	defer uploaderCancel()

	go func() {
		uploader.Start(uploaderCtx)
	}()

	// Start ingester (blocks until context is cancelled or fatal error occurs)
	if err := ingester.Start(ctx); err != nil && err != context.Canceled {
		log.Printf("[Client] [Stream %s] Ingester exited with error: %v", sc.StreamPath, err)
	}

	// Close chunk channel so uploader workers exit
	close(chunkChan)
}
