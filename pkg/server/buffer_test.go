package server

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
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

func TestBufferPopSkipsMissingChunkQuicklyWhenLaterChunksExist(t *testing.T) {
	tempDir := t.TempDir()
	buf := NewBuffer(&common.ServerConfig{BufferDuration: 2, ChunkDuration: 1}, tempDir)
	buf.Add(0, filepath.Join(tempDir, "chunk_0.ts"))
	buf.Add(2, filepath.Join(tempDir, "chunk_2.ts")) // chunk 1 never arrives

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, seq, err := buf.Pop(ctx); err != nil || seq != 0 {
		t.Fatalf("first Pop = seq %d, err %v; want seq 0", seq, err)
	}

	start := time.Now()
	if _, _, err := buf.Pop(ctx); err == nil {
		t.Fatal("expected a timeout error for the missing chunk")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("skipping a missing chunk took %v, want about one chunk duration", elapsed)
	}
	if _, seq, err := buf.Pop(ctx); err != nil || seq != 2 {
		t.Fatalf("Pop after skip = seq %d, err %v; want seq 2", seq, err)
	}
}

func TestBufferEvictsOldestChunksAboveCap(t *testing.T) {
	tempDir := t.TempDir()
	buf := NewBuffer(&common.ServerConfig{BufferDuration: 4, ChunkDuration: 2, MaxBufferedChunks: 3}, tempDir)

	paths := make([]string, 5)
	for i := range paths {
		paths[i] = filepath.Join(tempDir, fmt.Sprintf("chunk_%d.ts", i))
		if err := os.WriteFile(paths[i], []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
		buf.Add(int64(i), paths[i])
	}

	buf.mu.Lock()
	defer buf.mu.Unlock()
	if len(buf.chunks) != 3 {
		t.Fatalf("buffered chunks = %d, want 3", len(buf.chunks))
	}
	for i := 0; i < 2; i++ {
		if _, ok := buf.chunks[int64(i)]; ok {
			t.Errorf("oldest chunk %d was kept", i)
		}
		if _, err := os.Stat(paths[i]); !os.IsNotExist(err) {
			t.Errorf("evicted chunk file %d still on disk", i)
		}
	}
}

func TestBufferEvictionAdvancesPlaybackPosition(t *testing.T) {
	tempDir := t.TempDir()
	buf := NewBuffer(&common.ServerConfig{BufferDuration: 4, ChunkDuration: 2, MaxBufferedChunks: 2}, tempDir)
	buf.Add(0, filepath.Join(tempDir, "chunk_0.ts"))
	buf.Add(1, filepath.Join(tempDir, "chunk_1.ts"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := buf.WaitUntilReady(ctx); err != nil {
		t.Fatal(err)
	}
	// Output is stalled: more chunks arrive, pushing 0 and 1 out.
	buf.Add(2, filepath.Join(tempDir, "chunk_2.ts"))
	buf.Add(3, filepath.Join(tempDir, "chunk_3.ts"))

	start := time.Now()
	if _, seq, err := buf.Pop(ctx); err != nil || seq != 2 {
		t.Fatalf("Pop = seq %d, err %v; want seq 2 without waiting for dropped chunks", seq, err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Pop waited for chunks that were evicted")
	}
}

func TestBufferRequeueRestoresPlaybackPosition(t *testing.T) {
	tempDir := t.TempDir()
	buf := NewBuffer(&common.ServerConfig{BufferDuration: 4, ChunkDuration: 2}, tempDir)
	buf.Add(0, filepath.Join(tempDir, "chunk_0.ts"))
	buf.Add(1, filepath.Join(tempDir, "chunk_1.ts"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path, seq, err := buf.Pop(ctx)
	if err != nil || seq != 0 {
		t.Fatalf("Pop = seq %d, err %v; want 0", seq, err)
	}

	buf.Requeue(seq, path)
	if _, seq, err := buf.Pop(ctx); err != nil || seq != 0 {
		t.Fatalf("Pop after Requeue = seq %d, err %v; want 0 again", seq, err)
	}
}

func TestBufferPopWakesWhenChunkArrives(t *testing.T) {
	tempDir := t.TempDir()
	buf := NewBuffer(&common.ServerConfig{BufferDuration: 2, ChunkDuration: 1}, tempDir)
	buf.Add(0, filepath.Join(tempDir, "chunk_0.ts"))
	buf.Add(1, filepath.Join(tempDir, "chunk_1.ts"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if _, _, err := buf.Pop(ctx); err != nil {
			t.Fatal(err)
		}
	}

	go func() {
		time.Sleep(150 * time.Millisecond)
		buf.Add(2, filepath.Join(tempDir, "chunk_2.ts"))
	}()
	start := time.Now()
	if _, seq, err := buf.Pop(ctx); err != nil || seq != 2 {
		t.Fatalf("Pop = seq %d, err %v; want 2", seq, err)
	}
	if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
		t.Fatalf("Pop took %v to notice the new chunk", elapsed)
	}
}

func TestBufferLogsProgressWhileFilling(t *testing.T) {
	var logged bytes.Buffer
	log.SetOutput(&logged)
	defer log.SetOutput(os.Stderr)

	tempDir := t.TempDir()
	buf := NewBuffer(&common.ServerConfig{BufferDuration: 6, ChunkDuration: 2}, tempDir)
	buf.Add(0, filepath.Join(tempDir, "chunk_0.ts"))
	buf.Add(1, filepath.Join(tempDir, "chunk_1.ts"))

	if out := logged.String(); !strings.Contains(out, "Buffering before output starts: 1/3 chunks") || !strings.Contains(out, "2/3 chunks") {
		t.Fatalf("buffer progress not logged:\n%s", out)
	}
}
