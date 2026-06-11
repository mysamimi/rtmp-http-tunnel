package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

type Buffer struct {
	cfg        *common.ServerConfig
	tempDir    string
	mu         sync.Mutex
	chunks     map[int64]string // maps sequence number to local file path
	nextSeq    int64
	hasStarted bool
}

func NewBuffer(cfg *common.ServerConfig, tempDir string) *Buffer {
	return &Buffer{
		cfg:     cfg,
		tempDir: tempDir,
		chunks:  make(map[int64]string),
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
	log.Printf("[Buffer] Added chunk %d (Total buffered chunks: %d)", seqNum, len(b.chunks))
}

// Pop retrieves the next chunk in sequence. It blocks if the next chunk is not yet ready,
// or if the buffer is not yet filled to the required threshold.
func (b *Buffer) Pop(ctx context.Context) (string, int64, error) {
	b.mu.Lock()

	// 1. Wait until buffer has enough chunks to start
	requiredChunks := b.cfg.BufferDuration / b.cfg.ChunkDuration
	if requiredChunks <= 0 {
		requiredChunks = 15 // Default to 30s buffer if chunk duration is invalid
	}

	for !b.hasStarted {
		if len(b.chunks) >= requiredChunks {
			b.hasStarted = true
			// Find the lowest sequence number currently in buffer to start with
			var minSeq int64 = -1
			for seq := range b.chunks {
				if minSeq == -1 || seq < minSeq {
					minSeq = seq
				}
			}
			b.nextSeq = minSeq
			log.Printf("[Buffer] Buffer filled (%d/%d chunks). Starting streaming from Seq %d", len(b.chunks), requiredChunks, b.nextSeq)
			break
		}

		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", 0, ctx.Err()
		default:
			time.Sleep(200 * time.Millisecond)
		}
		b.mu.Lock()
	}

	// 2. Retrieve the next sequence chunk
	// We wait up to 4 times the chunk duration before skipping to prevent permanent freeze
	waitTimeout := time.Duration(b.cfg.ChunkDuration*4) * time.Second
	if waitTimeout < 4*time.Second {
		waitTimeout = 8 * time.Second
	}

	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()

	for {
		filePath, exists := b.chunks[b.nextSeq]
		if exists {
			delete(b.chunks, b.nextSeq)
			currentSeq := b.nextSeq
			b.nextSeq++
			b.mu.Unlock()
			return filePath, currentSeq, nil
		}

		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", 0, ctx.Err()
		case <-timer.C:
			b.mu.Lock()
			log.Printf("[Buffer] WARNING: Timeout waiting for chunk Seq %d. Skipping to keep stream alive...", b.nextSeq)
			skippedSeq := b.nextSeq
			b.nextSeq++

			// If buffer is completely empty, resethasStarted to force re-buffering
			if len(b.chunks) == 0 {
				log.Println("[Buffer] Buffer completely drained. Pausing stream to re-buffer...")
				b.hasStarted = false
			}

			b.mu.Unlock()
			return "", skippedSeq, fmt.Errorf("timeout waiting for sequence %d", skippedSeq)
		default:
			time.Sleep(100 * time.Millisecond)
		}
		b.mu.Lock()
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
	b.nextSeq = 0

	// Also clean up any loose .ts files in tempDir
	if b.tempDir != "" {
		files, err := os.ReadDir(b.tempDir)
		if err == nil {
			for _, f := range files {
				if strings.HasSuffix(f.Name(), ".ts") {
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
