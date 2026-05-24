package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"rtmp-http-tunnel/pkg/common"
)

type Uploader struct {
	cfg        *common.ClientConfig
	chunkChan  <-chan ChunkJob
	httpClient *http.Client
}

func NewUploader(cfg *common.ClientConfig, chunkChan <-chan ChunkJob) *Uploader {
	// Custom transport allowing self-signed certificates for ease of use with personal VPS
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	
	return &Uploader{
		cfg:       cfg,
		chunkChan: chunkChan,
		httpClient: &http.Client{
			Transport: tr,
			Timeout:   30 * time.Second,
		},
	}
}

// Start spawns the concurrent upload workers.
func (u *Uploader) Start(ctx context.Context) {
	var wg sync.WaitGroup
	
	concurrency := u.cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}

	log.Printf("[Uploader] Starting %d concurrent upload workers", concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			u.worker(ctx, workerID)
		}(i)
	}

	wg.Wait()
	log.Println("[Uploader] All upload workers stopped")
}

func (u *Uploader) worker(ctx context.Context, workerID int) {
	log.Printf("[Uploader-Worker-%d] Started", workerID)
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-u.chunkChan:
			if !ok {
				return
			}
			
			log.Printf("[Uploader-Worker-%d] Uploading chunk %s (Seq: %d)...", workerID, job.Filename, job.SeqNum)
			
			err := u.uploadWithRetry(ctx, job, 5)
			if err != nil {
				log.Printf("[Uploader-Worker-%d] ERROR: Failed to upload chunk %s after retries: %v", workerID, job.Filename, err)
				// Even if it fails, we should delete the local file to avoid disk build-up
				_ = os.Remove(job.Path)
			} else {
				log.Printf("[Uploader-Worker-%d] Uploaded chunk %s successfully.", workerID, job.Filename)
				// Delete local segment file after successful upload
				_ = os.Remove(job.Path)
			}
		}
	}
}

func (u *Uploader) uploadWithRetry(ctx context.Context, job ChunkJob, maxRetries int) error {
	var lastErr error
	backoff := 500 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := u.uploadChunk(ctx, job)
		if err == nil {
			return nil
		}

		lastErr = err
		log.Printf("[Uploader] Upload attempt %d failed for %s: %v. Retrying in %v...", attempt, job.Filename, err, backoff)
		
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2 // Exponential backoff
	}

	return fmt.Errorf("exhausted all retries: %w", lastErr)
}

func (u *Uploader) uploadChunk(ctx context.Context, job ChunkJob) error {
	// Read file contents
	data, err := os.ReadFile(job.Path)
	if err != nil {
		return fmt.Errorf("failed to read chunk file: %w", err)
	}

	// Apply XOR obfuscation
	obfuscatedData := common.XORApply(data, u.cfg.ObfuscationKey)

	// Create POST request
	url := fmt.Sprintf("%s/upload", u.cfg.ServerURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(obfuscatedData))
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}

	// Set custom headers for metadata & authentication
	req.Header.Set("Authorization", "Bearer "+u.cfg.AuthToken)
	req.Header.Set("X-Sequence-Number", strconv.FormatInt(job.SeqNum, 10))
	req.Header.Set("X-Filename", job.Filename)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := u.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned non-200 code: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}
