package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

var ErrBufferDrained = errors.New("buffer drained")

type Buffer struct {
	cfg            *common.ServerConfig
	tempDir        string
	mu             sync.Mutex
	chunks         map[int64]string // maps sequence number to local file path
	nextSeq        int64
	hasStarted     bool
	hasEverStarted bool
	notify         chan struct{} // signalled (non-blocking) whenever the set of chunks changes
}

func NewBuffer(cfg *common.ServerConfig, tempDir string) *Buffer {
	return &Buffer{
		cfg:     cfg,
		tempDir: tempDir,
		chunks:  make(map[int64]string),
		notify:  make(chan struct{}, 1),
	}
}

// signal wakes a goroutine waiting in Pop or WaitUntilReady.
func (b *Buffer) signal() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

// Add appends a new chunk file path to the buffer map.
func (b *Buffer) Add(seqNum int64, filePath string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Discard late-arriving chunks to prevent disk/memory leaks
	if b.hasStarted && seqNum < b.nextSeq {
		log.Printf("[Buffer] Discarding late chunk Seq %d (already passed nextSeq %d)", seqNum, b.nextSeq)
		_ = os.Remove(filePath)
		return
	}

	b.chunks[seqNum] = filePath
	if b.hasStarted {
		log.Printf("[Buffer] Added chunk %d (Total buffered chunks: %d)", seqNum, len(b.chunks))
	} else {
		required := b.requiredChunks(b.hasEverStarted)
		log.Printf("[Buffer] Buffering before output starts: %d/%d chunks (nothing is sent to the target until the buffer is full)", len(b.chunks), required)
	}
	b.evictOverflowLocked()
	b.signal()
}

// maxChunks is the most chunks kept on disk; beyond it the oldest are dropped so a
// stalled output cannot fill the disk.
func (b *Buffer) maxChunks() int {
	if b.cfg.MaxBufferedChunks > 0 {
		return b.cfg.MaxBufferedChunks
	}
	n := b.initialRequiredChunks() * 3
	if n < 30 {
		n = 30
	}
	return n
}

func (b *Buffer) evictOverflowLocked() {
	limit := b.maxChunks()
	if len(b.chunks) <= limit {
		return
	}
	dropped := 0
	for len(b.chunks) > limit {
		minSeq := int64(-1)
		for seq := range b.chunks {
			if minSeq == -1 || seq < minSeq {
				minSeq = seq
			}
		}
		_ = os.Remove(b.chunks[minSeq])
		delete(b.chunks, minSeq)
		dropped++
	}
	// Playback resumes from the oldest chunk we still have instead of waiting for dropped ones.
	if b.hasStarted {
		minSeq := int64(-1)
		for seq := range b.chunks {
			if minSeq == -1 || seq < minSeq {
				minSeq = seq
			}
		}
		if minSeq > b.nextSeq {
			b.nextSeq = minSeq
		}
	}
	log.Printf("[Buffer] WARNING: buffer exceeded %d chunks (output stalled?). Dropped %d oldest chunk(s)", limit, dropped)
}

// Requeue puts back a chunk that was popped but could not be delivered, so the next
// streamer run starts from it again.
func (b *Buffer) Requeue(seqNum int64, filePath string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.chunks[seqNum]; exists {
		return
	}
	b.chunks[seqNum] = filePath
	if b.hasStarted && seqNum < b.nextSeq {
		b.nextSeq = seqNum
	}
	b.evictOverflowLocked()
	b.signal()
}

// Pop retrieves the next chunk in sequence. It blocks if the next chunk is not yet ready,
// or if the buffer is not yet filled to the required threshold.
func (b *Buffer) Pop(ctx context.Context) (string, int64, error) {
	if err := b.WaitUntilReady(ctx); err != nil {
		return "", 0, err
	}

	// Retrieve the next sequence chunk.
	// We wait up to 4 times the chunk duration before skipping to prevent permanent freeze.
	// When later chunks are already buffered the missing one is most likely lost
	// (e.g. dropped by the client), so we only give it about one chunk duration.
	waitTimeout := time.Duration(b.cfg.ChunkDuration*4) * time.Second
	if waitTimeout < 4*time.Second {
		waitTimeout = 8 * time.Second
	}
	laterTimeout := time.Duration(b.cfg.ChunkDuration) * time.Second
	if laterTimeout < time.Second {
		laterTimeout = time.Second
	}

	start := time.Now()
	b.mu.Lock()
	for {
		filePath, exists := b.chunks[b.nextSeq]
		if exists {
			delete(b.chunks, b.nextSeq)
			currentSeq := b.nextSeq
			b.nextSeq++
			b.mu.Unlock()
			return filePath, currentSeq, nil
		}

		limit := waitTimeout
		if b.hasLaterChunkLocked() {
			limit = laterTimeout
		}
		if time.Since(start) >= limit {
			log.Printf("[Buffer] WARNING: Timeout waiting for chunk Seq %d. Skipping to keep stream alive...", b.nextSeq)
			skippedSeq := b.nextSeq
			b.nextSeq++

			// If buffer is completely empty, reset hasStarted to force re-buffering
			if len(b.chunks) == 0 {
				log.Println("[Buffer] Buffer completely drained. Pausing stream to re-buffer...")
				b.hasStarted = false
				b.mu.Unlock()
				return "", skippedSeq, fmt.Errorf("%w while waiting for sequence %d", ErrBufferDrained, skippedSeq)
			}

			b.mu.Unlock()
			return "", skippedSeq, fmt.Errorf("timeout waiting for sequence %d", skippedSeq)
		}

		b.mu.Unlock()
		// Sleep until a chunk arrives or the skip deadline passes. The one-second cap is a
		// safety net so a missed wake-up can only delay, never stall, playback.
		wait := limit - time.Since(start)
		if wait > time.Second {
			wait = time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", 0, ctx.Err()
		case <-b.notify:
		case <-timer.C:
		}
		timer.Stop()
		b.mu.Lock()
	}
}

func (b *Buffer) hasLaterChunkLocked() bool {
	for seq := range b.chunks {
		if seq > b.nextSeq {
			return true
		}
	}
	return false
}

// ShouldAccept reports whether a chunk with this sequence number is still useful:
// it is neither already buffered nor older than the playback position.
func (b *Buffer) ShouldAccept(seqNum int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.hasStarted && seqNum < b.nextSeq {
		return false
	}
	_, dup := b.chunks[seqNum]
	return !dup
}

// WaitUntilReady blocks until enough chunks are buffered to start or restart FFmpeg.
func (b *Buffer) WaitUntilReady(ctx context.Context) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		b.mu.Lock()
		if b.hasStarted {
			b.mu.Unlock()
			return nil
		}
		isResume := b.hasEverStarted
		requiredChunks := b.requiredChunks(isResume)
		if len(b.chunks) >= requiredChunks {
			b.startLocked(requiredChunks, isResume)
			b.mu.Unlock()
			return nil
		}
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.notify:
		case <-ticker.C:
		}
	}
}

func (b *Buffer) requiredChunks(isResume bool) int {
	if isResume {
		return b.rebufferRequiredChunks()
	}
	return b.initialRequiredChunks()
}

func (b *Buffer) initialRequiredChunks() int {
	requiredChunks := ceilDiv(b.cfg.BufferDuration, b.cfg.ChunkDuration)
	if requiredChunks <= 0 {
		return 15 // Default to 30s buffer if chunk duration is invalid
	}
	return requiredChunks
}

func (b *Buffer) rebufferRequiredChunks() int {
	duration := b.cfg.RebufferDuration
	if duration <= 0 {
		duration = b.cfg.ChunkDuration * 2
	}
	requiredChunks := ceilDiv(duration, b.cfg.ChunkDuration)
	if requiredChunks <= 0 {
		requiredChunks = 1
	}
	initialChunks := b.initialRequiredChunks()
	if requiredChunks > initialChunks {
		return initialChunks
	}
	return requiredChunks
}

func ceilDiv(a, b int) int {
	if a <= 0 || b <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

func (b *Buffer) startLocked(requiredChunks int, isResume bool) {
	b.hasStarted = true
	b.hasEverStarted = true
	var minSeq int64 = -1
	for seq := range b.chunks {
		if minSeq == -1 || seq < minSeq {
			minSeq = seq
		}
	}
	b.nextSeq = minSeq
	if isResume {
		log.Printf("[Buffer] Rebuffer filled (%d/%d chunks). Resuming streaming from Seq %d", len(b.chunks), requiredChunks, b.nextSeq)
	} else {
		log.Printf("[Buffer] Buffer filled (%d/%d chunks). Starting streaming from Seq %d", len(b.chunks), requiredChunks, b.nextSeq)
	}
}

// Reset clears the buffer and cleans up untransmitted files on disk
func (b *Buffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Clean up any files tracked in the map
	for _, path := range b.chunks {
		_ = os.Remove(path)
	}

	b.chunks = make(map[int64]string)
	b.hasStarted = false
	b.hasEverStarted = false
	b.nextSeq = 0

	// Also clean up any loose .ts files in tempDir
	if b.tempDir != "" {
		files, err := os.ReadDir(b.tempDir)
		if err == nil {
			for _, f := range files {
				if strings.HasSuffix(f.Name(), ".ts") || strings.HasSuffix(f.Name(), ".part") {
					_ = os.Remove(filepath.Join(b.tempDir, f.Name()))
				}
			}
		}
	}
	log.Println("[Buffer] Buffer has been reset and temp files cleared")
}

// Cleanup releases all resources associated with the buffer
func (b *Buffer) Cleanup() {
	b.Reset()
}
