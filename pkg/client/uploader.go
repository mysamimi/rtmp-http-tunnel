package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

type Uploader struct {
	cfg         *common.ClientConfig
	stream      common.StreamConfig
	chunkChan   <-chan ChunkJob
	httpClient  *http.Client
	maxChunkAge time.Duration
}

func NewUploader(cfg *common.ClientConfig, stream common.StreamConfig, chunkChan <-chan ChunkJob) *Uploader {
	// Custom transport allowing self-signed certificates for ease of use with personal VPS
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	return &Uploader{
		cfg:       cfg,
		stream:    stream,
		chunkChan: chunkChan,
		httpClient: &http.Client{
			Transport: tr,
			Timeout:   30 * time.Second,
		},
	}
}

func (u *Uploader) discoverBufferConfig(ctx context.Context) {
	url := fmt.Sprintf("%s/s/%s/health", u.cfg.ServerURL, u.stream.StreamPath)

	// Fallback defaults
	defaultBufferDuration := 60
	defaultChunkDuration := 2
	u.maxChunkAge = time.Duration(defaultBufferDuration-defaultChunkDuration) * time.Second

	log.Printf("[Uploader %s] Querying health endpoint for buffer configuration: %s", u.stream.StreamPath, url)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("[Uploader %s] Failed to create health check request: %v. Using fallback max age %v.", u.stream.StreamPath, err, u.maxChunkAge)
		return
	}
	req.Header.Set("Authorization", "Bearer "+u.cfg.AuthToken)

	resp, err := u.httpClient.Do(req)
	if err != nil {
		log.Printf("[Uploader %s] Health check request failed: %v. Using fallback max age %v.", u.stream.StreamPath, err, u.maxChunkAge)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[Uploader %s] Health check returned status %d. Using fallback max age %v.", u.stream.StreamPath, resp.StatusCode, u.maxChunkAge)
		return
	}

	var result struct {
		BufferDuration int `json:"buffer_duration"`
		ChunkDuration  int `json:"chunk_duration"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("[Uploader %s] Failed to parse health check response: %v. Using fallback max age %v.", u.stream.StreamPath, err, u.maxChunkAge)
		return
	}

	bufferDur := result.BufferDuration
	chunkDur := result.ChunkDuration
	if bufferDur <= 0 {
		bufferDur = defaultBufferDuration
	}
	if chunkDur <= 0 {
		chunkDur = defaultChunkDuration
	}
	if bufferDur <= chunkDur {
		log.Printf("[Uploader %s] Invalid server buffer settings (buffer_duration=%ds, chunk_duration=%ds). Using fallback max age %v.",
			u.stream.StreamPath, bufferDur, chunkDur, u.maxChunkAge)
		return
	}

	u.maxChunkAge = time.Duration(bufferDur-chunkDur) * time.Second
	log.Printf("[Uploader %s] Successfully discovered buffer configuration from server: buffer_duration=%ds, chunk_duration=%ds. Calculated maxChunkAge: %v",
		u.stream.StreamPath, bufferDur, chunkDur, u.maxChunkAge)
}

// Start spawns the concurrent upload workers.
func (u *Uploader) Start(ctx context.Context) {
	// Query server health for buffer config before starting workers
	u.discoverBufferConfig(ctx)

	var wg sync.WaitGroup

	concurrency := u.cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}

	log.Printf("[Uploader %s] Starting %d concurrent upload workers", u.stream.StreamPath, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			u.worker(ctx, workerID)
		}(i)
	}

	wg.Wait()
	log.Printf("[Uploader %s] All upload workers stopped", u.stream.StreamPath)
}

func (u *Uploader) worker(ctx context.Context, workerID int) {
	log.Printf("[Uploader-Worker-%d-%s] Started", workerID, u.stream.StreamPath)
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-u.chunkChan:
			if !ok {
				return
			}

			if ctx.Err() != nil {
				return
			}

			log.Printf("[Uploader-Worker-%d-%s] Uploading chunk %s (Seq: %d)...", workerID, u.stream.StreamPath, job.Filename, job.SeqNum)

			maxRetries := u.cfg.MaxUploadRetries
			if maxRetries <= 0 {
				maxRetries = 5
			}

			err := u.uploadWithRetry(ctx, job, maxRetries)
			if err != nil {
				log.Printf("[Uploader-Worker-%d-%s] ERROR: Failed to upload chunk %s after retries: %v", workerID, u.stream.StreamPath, job.Filename, err)
				_ = os.Remove(job.Path)
			} else {
				log.Printf("[Uploader-Worker-%d-%s] Processed chunk %s.", workerID, u.stream.StreamPath, job.Filename)
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

		// Smart Expiry Check
		age := time.Since(job.CreatedAt)
		if age > u.maxChunkAge {
			log.Printf("[Uploader %s] Chunk Seq %d EXPIRED (age: %.1fs > max: %.1fs). Skipping upload.",
				u.stream.StreamPath, job.SeqNum, age.Seconds(), u.maxChunkAge.Seconds())
			return nil // Return nil so it's not logged as a failed chunk error to clean up the file
		}

		err := u.uploadChunk(ctx, job)
		if err == nil {
			return nil
		}

		lastErr = err
		log.Printf("[Uploader %s] Upload attempt %d failed for %s: %v. Retrying in %v...",
			u.stream.StreamPath, attempt, job.Filename, err, backoff)

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

	// Create POST request to stream specific endpoint
	url := fmt.Sprintf("%s/s/%s/upload", u.cfg.ServerURL, u.stream.StreamPath)
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
