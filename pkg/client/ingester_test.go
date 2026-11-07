package client

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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

func TestIngesterCleanupKeepsSequenceState(t *testing.T) {
	tempDir := t.TempDir()
	statePath := filepath.Join(tempDir, ".rtmp-http-tunnel-next-seq")
	for name, content := range map[string]string{
		".rtmp-http-tunnel-next-seq": "42",
		"chunk_0.ts":                 "x",
		"index.m3u8":                 "x",
	} {
		if err := os.WriteFile(filepath.Join(tempDir, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}

	ingester := NewIngester(&common.ClientConfig{}, common.StreamConfig{StreamPath: "s", TempDir: tempDir}, make(chan ChunkJob, 1))
	ingester.cleanupTempDir()

	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("sequence state file removed by cleanup: %v", err)
	}
	for _, name := range []string{"chunk_0.ts", "index.m3u8"} {
		if _, err := os.Stat(filepath.Join(tempDir, name)); !os.IsNotExist(err) {
			t.Fatalf("%s should have been removed", name)
		}
	}
}

func TestRunStreamPreservesSequenceState(t *testing.T) {
	tempDir := t.TempDir()
	statePath := filepath.Join(tempDir, ".rtmp-http-tunnel-next-seq")
	if err := os.WriteFile(statePath, []byte("42"), 0644); err != nil {
		t.Fatal(err)
	}

	// Cancelled context: runStream sets up the directory and returns without spawning FFmpeg.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runStream(ctx, &common.ClientConfig{ChunkDuration: 2, ServerURL: "http://127.0.0.1:1"},
		common.StreamConfig{StreamPath: "s", InputURL: "rtmp://0.0.0.0:1935", TempDir: tempDir})

	data, err := os.ReadFile(statePath)
	if err != nil || string(data) != "42" {
		t.Fatalf("sequence state after runStream = %q, %v; want 42", data, err)
	}
}

func TestMonitorDrainQueuesFinalSegment(t *testing.T) {
	tempDir := t.TempDir()
	playlistPath := filepath.Join(tempDir, "index.m3u8")
	chunkChan := make(chan ChunkJob, 4)
	ingester := NewIngester(&common.ClientConfig{}, common.StreamConfig{StreamPath: "s", TempDir: tempDir}, chunkChan)
	seqRegexp := regexp.MustCompile(`chunk_(\d+)\.ts`)

	drain := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ingester.monitorPlaylistDrain(context.Background(), drain, playlistPath, seqRegexp)
	}()

	// Playlist appears right before FFmpeg exits, faster than the poll interval.
	writePlaylist(t, playlistPath, "r1_chunk_0.ts")
	close(drain)
	<-done

	if job := receiveChunkJob(t, chunkChan); job.Path != filepath.Join(tempDir, "r1_chunk_0.ts") {
		t.Fatalf("queued path = %q", job.Path)
	}
}

func TestScanPlaylistUsesSegmentModTimeForAge(t *testing.T) {
	tempDir := t.TempDir()
	playlistPath := filepath.Join(tempDir, "index.m3u8")
	chunkChan := make(chan ChunkJob, 1)
	ingester := NewIngester(&common.ClientConfig{}, common.StreamConfig{StreamPath: "s", TempDir: tempDir}, chunkChan)

	segment := filepath.Join(tempDir, "r1_chunk_0.ts")
	if err := os.WriteFile(segment, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	written := time.Now().Add(-45 * time.Second)
	if err := os.Chtimes(segment, written, written); err != nil {
		t.Fatal(err)
	}
	writePlaylist(t, playlistPath, "r1_chunk_0.ts")

	last := int64(-1)
	ingester.scanPlaylist(context.Background(), playlistPath, regexp.MustCompile(`chunk_(\d+)\.ts`), &last)
	job := receiveChunkJob(t, chunkChan)
	if age := time.Since(job.CreatedAt); age < 44*time.Second {
		t.Fatalf("chunk age = %v, want about 45s (segment write time, not queue time)", age)
	}
}

func TestWarnIfSegmentTooLong(t *testing.T) {
	ingester := NewIngester(&common.ClientConfig{ChunkDuration: 2}, common.StreamConfig{StreamPath: "s", TempDir: t.TempDir()}, make(chan ChunkJob, 1))

	ingester.warnIfSegmentTooLong(2.04)
	if ingester.warnedLongSegment {
		t.Fatal("warned for a segment within tolerance")
	}
	ingester.warnIfSegmentTooLong(6.0)
	if !ingester.warnedLongSegment {
		t.Fatal("did not warn for a 6s segment with chunk_duration 2")
	}
}

func TestScanPlaylistReadsExtinfDurations(t *testing.T) {
	tempDir := t.TempDir()
	playlistPath := filepath.Join(tempDir, "index.m3u8")
	ingester := NewIngester(&common.ClientConfig{ChunkDuration: 2}, common.StreamConfig{StreamPath: "s", TempDir: tempDir}, make(chan ChunkJob, 2))
	content := "#EXTM3U\n#EXT-X-VERSION:3\n#EXTINF:6.000000,\nr1_chunk_0.ts\n"
	if err := os.WriteFile(playlistPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	last := int64(-1)
	ingester.scanPlaylist(context.Background(), playlistPath, regexp.MustCompile(`chunk_(\d+)\.ts`), &last)
	if !ingester.warnedLongSegment {
		t.Fatal("6s EXTINF segment did not trigger the keyframe warning")
	}
}

func TestLogFFmpegOutputSplitsLinesAndDropsProgress(t *testing.T) {
	var logged bytes.Buffer
	log.SetOutput(&logged)
	defer log.SetOutput(os.Stderr)

	ingester := NewIngester(&common.ClientConfig{}, common.StreamConfig{StreamPath: "s"}, make(chan ChunkJob, 1))
	input := "Input #0, flv\r\nframe=  10 fps= 25 q=-1.0 size=  100KiB time=00:00:00.40 speed=1.0x    \rframe=  20 fps= 25 size= 200KiB speed=1.0x\rError while decoding\nlast without newline"
	ingester.logFFmpegOutput(strings.NewReader(input))

	out := logged.String()
	for _, want := range []string{"[FFmpeg-s] Input #0, flv", "[FFmpeg-s] Error while decoding", "[FFmpeg-s] last without newline"} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "frame=") {
		t.Errorf("progress lines were not dropped:\n%s", out)
	}
}
