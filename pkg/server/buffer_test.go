package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

func TestBufferWaitUntilReadyStartsFromLowestSequence(t *testing.T) {
	tempDir := t.TempDir()
	buf := NewBuffer(&common.ServerConfig{
		BufferDuration: 4,
		ChunkDuration:  2,
	}, tempDir)

	buf.Add(11, filepath.Join(tempDir, "chunk_11.ts"))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	if err := buf.WaitUntilReady(ctx); err == nil {
		t.Fatal("WaitUntilReady returned nil before required chunks were buffered")
	}
	cancel()

	buf.Add(10, filepath.Join(tempDir, "chunk_10.ts"))

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := buf.WaitUntilReady(ctx); err != nil {
		t.Fatalf("WaitUntilReady returned error after required chunks were buffered: %v", err)
	}

	if !buf.hasStarted {
		t.Fatal("buffer did not mark itself as started")
	}
	if buf.nextSeq != 10 {
		t.Fatalf("nextSeq = %d, want 10", buf.nextSeq)
	}
}

func TestBufferWaitUntilReadyUsesSmallerThresholdForResume(t *testing.T) {
	tempDir := t.TempDir()
	buf := NewBuffer(&common.ServerConfig{
		BufferDuration:   100,
		RebufferDuration: 4,
		ChunkDuration:    2,
	}, tempDir)
	buf.hasEverStarted = true

	buf.Add(101, filepath.Join(tempDir, "chunk_101.ts"))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	if err := buf.WaitUntilReady(ctx); err == nil {
		t.Fatal("WaitUntilReady returned nil before resume threshold was buffered")
	}
	cancel()

	buf.Add(100, filepath.Join(tempDir, "chunk_100.ts"))

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := buf.WaitUntilReady(ctx); err != nil {
		t.Fatalf("WaitUntilReady returned error after resume threshold was buffered: %v", err)
	}

	if !buf.hasStarted {
		t.Fatal("buffer did not mark itself as started")
	}
	if buf.nextSeq != 100 {
		t.Fatalf("nextSeq = %d, want 100", buf.nextSeq)
	}
}
