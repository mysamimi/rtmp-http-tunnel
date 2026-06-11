package client

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

func TestIngesterUploadSequenceContinuesAcrossMonitorRestarts(t *testing.T) {
	tempDir := t.TempDir()
	playlistPath := filepath.Join(tempDir, "index.m3u8")
	chunkChan := make(chan ChunkJob, 2)
	ingester := NewIngester(&common.ClientConfig{}, common.StreamConfig{
		StreamPath: "test_stream",
		TempDir:    tempDir,
	}, chunkChan)
	seqRegexp := regexp.MustCompile(`chunk_(\d+)\.ts`)

	writePlaylist(t, playlistPath, "chunk_0.ts")
	ctx1, cancel1 := context.WithCancel(context.Background())
	go ingester.monitorPlaylist(ctx1, playlistPath, seqRegexp)
	first := receiveChunkJob(t, chunkChan)
	cancel1()

	if first.SeqNum != 0 {
		t.Fatalf("first upload sequence = %d, want 0", first.SeqNum)
	}
	if first.Filename != "chunk_0.ts" {
		t.Fatalf("first upload filename = %q, want chunk_0.ts", first.Filename)
	}

	writePlaylist(t, playlistPath, "chunk_0.ts")
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go ingester.monitorPlaylist(ctx2, playlistPath, seqRegexp)
	second := receiveChunkJob(t, chunkChan)

	if second.SeqNum != 1 {
		t.Fatalf("second upload sequence = %d, want 1", second.SeqNum)
	}
	if second.Filename != "chunk_1.ts" {
		t.Fatalf("second upload filename = %q, want chunk_1.ts", second.Filename)
	}
}

func TestIngesterUploadSequencePersistsAcrossClientRestarts(t *testing.T) {
	tempDir := t.TempDir()
	statePath := filepath.Join(tempDir, ".rtmp-http-tunnel-next-seq")

	first := NewIngester(&common.ClientConfig{}, common.StreamConfig{
		StreamPath: "test_stream",
		TempDir:    tempDir,
	}, make(chan ChunkJob, 1))
	first.sequenceStatePath = statePath

	if seq := first.nextSequence(); seq != 0 {
		t.Fatalf("first sequence = %d, want 0", seq)
	}
	if seq := first.nextSequence(); seq != 1 {
		t.Fatalf("second sequence = %d, want 1", seq)
	}

	restarted := NewIngester(&common.ClientConfig{}, common.StreamConfig{
		StreamPath: "test_stream",
		TempDir:    tempDir,
	}, make(chan ChunkJob, 1))
	restarted.sequenceStatePath = statePath
	restarted.loadSequenceState()

	if seq := restarted.nextSequence(); seq != 2 {
		t.Fatalf("sequence after restart = %d, want 2", seq)
	}
}

func writePlaylist(t *testing.T, path, chunk string) {
	t.Helper()
	content := "#EXTM3U\n#EXT-X-VERSION:3\n" + chunk + "\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write playlist: %v", err)
	}
}

func receiveChunkJob(t *testing.T, ch <-chan ChunkJob) ChunkJob {
	t.Helper()
	select {
	case job := <-ch:
		return job
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for chunk job")
		return ChunkJob{}
	}
}
