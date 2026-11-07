package client

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

func TestUploadWithRetryDoesNotRetryMissingFile(t *testing.T) {
	u := NewUploader(&common.ClientConfig{ServerURL: "http://127.0.0.1:1"}, common.StreamConfig{StreamPath: "s"}, make(chan ChunkJob))
	u.maxChunkAge = time.Minute

	start := time.Now()
	err := u.uploadWithRetry(context.Background(), ChunkJob{
		Path: filepath.Join(t.TempDir(), "gone.ts"), SeqNum: 1, Filename: "chunk_1.ts", CreatedAt: time.Now(),
	}, 5)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want ErrNotExist", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("missing file was retried (took %v)", time.Since(start))
	}
}
