package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

const (
	defaultBufferDuration = 60
	defaultChunkDuration  = 2
)

// discoveryRetryEvery is how often an unreachable server is asked for its settings again.
var discoveryRetryEvery = 30 * time.Second

type Uploader struct {
	cfg        *common.ClientConfig
	stream     common.StreamConfig
	chunkChan  <-chan ChunkJob
	httpClient *http.Client
	maxAge     atomic.Int64 // time.Duration; updated when the server settings are (re)discovered

	backlogMu      sync.Mutex
	lastBacklogLog time.Time
}

func NewUploader(cfg *common.ClientConfig, stream common.StreamConfig, chunkChan <-chan ChunkJob) (*Uploader, error) {
	tlsCfg, err := newTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.InsecureSkipVerify && cfg.TLSPinSHA256 == "" {
		log.Printf("[Uploader %s] WARNING: insecure_skip_verify is enabled; the server certificate is NOT verified. Prefer tls_pin_sha256.", stream.StreamPath)
	}

	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	// Keep one idle connection per worker so chunks reuse TLS sessions instead of re-handshaking.
	tr := &http.Transport{
		TLSClientConfig:     tlsCfg,
		MaxIdleConnsPerHost: concurrency,
		IdleConnTimeout:     90 * time.Second,
	}

	return &Uploader{
		cfg:       cfg,
		stream:    stream,
		chunkChan: chunkChan,
		httpClient: &http.Client{
			Transport: tr,
			Timeout:   30 * time.Second,
		},
	}, nil
}

// discoverBufferConfig asks the server for its buffer settings and derives the chunk
// expiry from them. It reports whether the settings were obtained.
func (u *Uploader) discoverBufferConfig(ctx context.Context) bool {
	url := fmt.Sprintf("%s/s/%s/health", u.cfg.ServerURL, u.stream.StreamPath)

	log.Printf("[Uploader %s] Querying health endpoint for buffer configuration: %s", u.stream.StreamPath, url)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("[Uploader %s] Failed to create health check request: %v. Using fallback max age %v.", u.stream.StreamPath, err, u.maxChunkAge())
		return false
	}
	req.Header.Set("Authorization", "Bearer "+u.cfg.AuthToken)

	resp, err := u.httpClient.Do(req)
	if err != nil {
		log.Printf("[Uploader %s] Health check request failed: %v. Using fallback max age %v.", u.stream.StreamPath, err, u.maxChunkAge())
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[Uploader %s] Health check returned status %d%s. Using fallback max age %v.", u.stream.StreamPath, resp.StatusCode, statusHint(resp.StatusCode), u.maxChunkAge())
		return false
	}

	var result struct {
		BufferDuration int `json:"buffer_duration"`
		ChunkDuration  int `json:"chunk_duration"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("[Uploader %s] Failed to parse health check response: %v. Using fallback max age %v.", u.stream.StreamPath, err, u.maxChunkAge())
		return false
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
			u.stream.StreamPath, bufferDur, chunkDur, u.maxChunkAge())
		return false
	}

	if chunkDur != u.cfg.ChunkDuration {
		log.Printf("[Uploader %s] WARNING: client chunk_duration (%ds) differs from server chunk_duration (%ds); the server's buffer sizing assumes its own value. Use the same value on both sides.",
			u.stream.StreamPath, u.cfg.ChunkDuration, chunkDur)
	}

	u.setMaxChunkAge(time.Duration(bufferDur-chunkDur) * time.Second)
	log.Printf("[Uploader %s] Successfully discovered buffer configuration from server: buffer_duration=%ds, chunk_duration=%ds. Calculated maxChunkAge: %v",
		u.stream.StreamPath, bufferDur, chunkDur, u.maxChunkAge())
	return true
}

// Start spawns the concurrent upload workers.
func (u *Uploader) Start(ctx context.Context) {
	// Query server health for buffer config before starting workers. Until it succeeds
	// a conservative fallback is used; if the server is unreachable now, keep trying.
	u.setMaxChunkAge(time.Duration(defaultBufferDuration-defaultChunkDuration) * time.Second)
	if !u.discoverBufferConfig(ctx) {
		go u.retryDiscovery(ctx)
	}

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

			u.warnOnBacklog()
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
		if age > u.maxChunkAge() {
			log.Printf("[Uploader %s] Chunk Seq %d EXPIRED (age: %.1fs > max: %.1fs). Skipping upload.",
				u.stream.StreamPath, job.SeqNum, age.Seconds(), u.maxChunkAge().Seconds())
			return nil // Return nil so it's not logged as a failed chunk error to clean up the file
		}

		err := u.uploadChunk(ctx, job)
		if err == nil {
			return nil
		}

		if errors.Is(err, os.ErrNotExist) {
			// The chunk file is gone; retrying cannot help.
			return fmt.Errorf("chunk file missing: %w", err)
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
		return fmt.Errorf("server returned non-200 code: %d, body: %s%s", resp.StatusCode, string(body), statusHint(resp.StatusCode))
	}

	return nil
}

// statusHint explains the deliberately uninformative 404 the server returns for
// unauthenticated requests, so misconfiguration is still easy to diagnose.
func statusHint(code int) string {
	if code == http.StatusNotFound {
		return " (404 can mean a wrong auth_token, an unknown stream_path, or a wrong server_url)"
	}
	return ""
}

// warnOnBacklog logs (at most every 10s) when the queue of chunks waiting for upload
// holds so much stream time that chunks are about to expire, which means the uplink is
// slower than the stream bitrate.
func (u *Uploader) warnOnBacklog() {
	backlog := len(u.chunkChan)
	queued := time.Duration(backlog*u.cfg.ChunkDuration) * time.Second
	maxAge := u.maxChunkAge()
	if backlog == 0 || maxAge <= 0 || queued < maxAge/2 {
		return
	}

	u.backlogMu.Lock()
	defer u.backlogMu.Unlock()
	if time.Since(u.lastBacklogLog) < 10*time.Second {
		return
	}
	u.lastBacklogLog = time.Now()
	log.Printf("[Uploader %s] WARNING: %d chunks (~%v of stream) are waiting for upload; chunks older than %v are dropped. The uplink is slower than the stream bitrate: lower the encoder bitrate or raise concurrency.",
		u.stream.StreamPath, backlog, queued, maxAge)
}

func (u *Uploader) maxChunkAge() time.Duration { return time.Duration(u.maxAge.Load()) }

func (u *Uploader) setMaxChunkAge(d time.Duration) { u.maxAge.Store(int64(d)) }

// retryDiscovery keeps asking the server for its buffer settings until it answers.
func (u *Uploader) retryDiscovery(ctx context.Context) {
	ticker := time.NewTicker(discoveryRetryEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if u.discoverBufferConfig(ctx) {
				return
			}
		}
	}
}
