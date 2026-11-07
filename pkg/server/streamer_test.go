package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

// A fake ffmpeg that reads a single byte and exits makes the pipe break on the
// first chunk. The remaining buffered chunks must be preserved for the reconnect.
func TestStreamerKeepsBufferWhenOutputDies(t *testing.T) {
	binDir := t.TempDir()
	script := "#!/bin/sh\nhead -c 1 >/dev/null\nexit 1\n"
	if err := os.WriteFile(filepath.Join(binDir, "ffmpeg"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	tempDir := t.TempDir()
	cfg := &common.ServerConfig{BufferDuration: 10, ChunkDuration: 2}
	buf := NewBuffer(cfg, tempDir)
	const total = 5
	payload := make([]byte, 512*1024) // larger than the pipe buffer so the write blocks until ffmpeg exits
	for i := 0; i < total; i++ {
		p := filepath.Join(tempDir, "chunk_"+string(rune('0'+i))+".ts")
		if err := os.WriteFile(p, payload, 0644); err != nil {
			t.Fatal(err)
		}
		buf.Add(int64(i), p)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = NewStreamer(cfg, "rtmp://example.invalid/live/key", buf).Start(ctx)
	}()

	// First run fails quickly; the restart delay is 2s, so inspect inside that window.
	time.Sleep(1 * time.Second)

	buf.mu.Lock()
	remaining, started, ever := len(buf.chunks), buf.hasStarted, buf.hasEverStarted
	buf.mu.Unlock()
	cancel()
	<-done

	if remaining != total-1 {
		t.Fatalf("buffered chunks after output failure = %d, want %d", remaining, total-1)
	}
	if !started || !ever {
		t.Fatalf("buffer state reset after output failure (hasStarted=%v, hasEverStarted=%v)", started, ever)
	}
}

func TestStartSessionStreamerStaleExitDoesNotClearNewStreamer(t *testing.T) {
	cfg := &common.ServerConfig{BufferDuration: 10, ChunkDuration: 2}
	buf := NewBuffer(cfg, t.TempDir())
	session := &StreamSession{Buffer: buf, Streamer: NewStreamer(cfg, "rtmp://x", buf), Path: "p"}
	s := &Server{cfg: cfg}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.StartSessionStreamer(ctx, session)
	session.Mu.Lock()
	oldCancel := session.CancelFunc
	session.CancelFunc = nil // simulate the inactivity cleanup
	session.Mu.Unlock()

	s.StartSessionStreamer(ctx, session) // new upload arrives, starts a second generation
	oldCancel()                          // old goroutine exits now

	time.Sleep(200 * time.Millisecond)
	session.Mu.Lock()
	defer session.Mu.Unlock()
	if session.CancelFunc == nil {
		t.Fatal("stale streamer exit cleared the CancelFunc of the newer streamer")
	}
}
