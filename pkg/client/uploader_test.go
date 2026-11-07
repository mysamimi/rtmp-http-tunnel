package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

func TestUploadWithRetryDoesNotRetryMissingFile(t *testing.T) {
	u, err := NewUploader(&common.ClientConfig{ServerURL: "http://127.0.0.1:1"}, common.StreamConfig{StreamPath: "s"}, make(chan ChunkJob))
	if err != nil {
		t.Fatal(err)
	}
	u.setMaxChunkAge(time.Minute)

	start := time.Now()
	err = u.uploadWithRetry(context.Background(), ChunkJob{
		Path: filepath.Join(t.TempDir(), "gone.ts"), SeqNum: 1, Filename: "chunk_1.ts", CreatedAt: time.Now(),
	}, 5)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want ErrNotExist", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("missing file was retried (took %v)", time.Since(start))
	}
}

func TestWarnOnBacklogOnlyWhenQueueIsDeep(t *testing.T) {
	ch := make(chan ChunkJob, 40)
	u, err := NewUploader(&common.ClientConfig{ChunkDuration: 2}, common.StreamConfig{StreamPath: "s"}, ch)
	if err != nil {
		t.Fatal(err)
	}
	u.setMaxChunkAge(58 * time.Second)

	for i := 0; i < 5; i++ { // ~10s of stream: fine
		ch <- ChunkJob{}
	}
	u.warnOnBacklog()
	if !u.lastBacklogLog.IsZero() {
		t.Fatal("warned for a shallow backlog")
	}

	for i := 0; i < 20; i++ { // ~50s of stream, close to the 58s expiry
		ch <- ChunkJob{}
	}
	u.warnOnBacklog()
	if u.lastBacklogLog.IsZero() {
		t.Fatal("did not warn for a deep backlog")
	}
}

func TestRetryDiscoveryPicksUpServerSettingsLater(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok","buffer_duration":30,"chunk_duration":2}`))
	}))
	defer srv.Close()

	old := discoveryRetryEvery
	discoveryRetryEvery = 20 * time.Millisecond
	defer func() { discoveryRetryEvery = old }()

	u, err := NewUploader(&common.ClientConfig{ServerURL: srv.URL, ChunkDuration: 2}, common.StreamConfig{StreamPath: "s"}, make(chan ChunkJob))
	if err != nil {
		t.Fatal(err)
	}
	u.setMaxChunkAge(58 * time.Second)

	if u.discoverBufferConfig(context.Background()) {
		t.Fatal("discovery reported success against a failing server")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.retryDiscovery(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for u.maxChunkAge() != 28*time.Second {
		if time.Now().After(deadline) {
			t.Fatalf("maxChunkAge = %v, want 28s after retry", u.maxChunkAge())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
