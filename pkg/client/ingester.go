package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"rtmp-http-tunnel/pkg/common"
)

type Ingester struct {
	cfg      *common.ClientConfig
	chunkChan chan<- ChunkJob
}

type ChunkJob struct {
	Path     string
	SeqNum   int64
	Filename string
}

func NewIngester(cfg *common.ClientConfig, chunkChan chan<- ChunkJob) *Ingester {
	return &Ingester{
		cfg:      cfg,
		chunkChan: chunkChan,
	}
}

// Start runs the FFmpeg RTMP listener and HLS segmenter loop.
func (in *Ingester) Start(ctx context.Context) error {
	// Create temp directory if it doesn't exist
	if err := os.MkdirAll(in.cfg.TempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	// We clean up any old chunk files in the temp directory at startup
	in.cleanupTempDir()

	playlistPath := filepath.Join(in.cfg.TempDir, "index.m3u8")

	log.Printf("[Ingester] Starting local RTMP listener on rtmp://%s", in.cfg.LocalRTMPAddr)
	log.Printf("[Ingester] Storing temporary HLS chunks in %s", in.cfg.TempDir)

	// Regexp to extract sequence number from chunk file name (e.g., chunk_12.ts)
	seqRegexp := regexp.MustCompile(`chunk_(\d+)\.ts`)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Prepare ffmpeg command
		// ffmpeg -listen 1 -i rtmp://<addr> -c copy -f hls -hls_time <duration> -hls_list_size 999999 -hls_segment_filename <temp_dir>/chunk_%d.ts <temp_dir>/index.m3u8
		// Using -listen 1 enables single-run RTMP listener mode. When client disconnects, ffmpeg exits.
		cmd := exec.CommandContext(ctx, "ffmpeg",
			"-loglevel", "info",
			"-listen", "1",
			"-i", fmt.Sprintf("rtmp://%s", in.cfg.LocalRTMPAddr),
			"-c", "copy",
			"-f", "hls",
			"-hls_time", strconv.Itoa(in.cfg.ChunkDuration),
			"-hls_list_size", "9999999", // keep index long enough
			"-hls_segment_filename", filepath.Join(in.cfg.TempDir, "chunk_%d.ts"),
			playlistPath,
		)

		// Capture stderr for logging if needed
		stderr, err := cmd.StderrPipe()
		if err != nil {
			log.Printf("[Ingester] Failed to pipe ffmpeg stderr: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if err := cmd.Start(); err != nil {
			log.Printf("[Ingester] Failed to start FFmpeg: %v. Is ffmpeg installed?", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Println("[Ingester] FFmpeg listener started, waiting for stream connection...")

		// Run stderr reader in goroutine to log stream state
		go func() {
			buf := make([]byte, 1024)
			for {
				n, err := stderr.Read(buf)
				if n > 0 {
					// You can uncomment this if you need ffmpeg detailed debug logs
					// log.Printf("[FFmpeg] %s", string(buf[:n]))
				}
				if err != nil {
					break
				}
			}
		}()

		// Start monitoring the index.m3u8 playlist file while FFmpeg runs
		monitorCtx, monitorCancel := context.WithCancel(ctx)
		go in.monitorPlaylist(monitorCtx, playlistPath, seqRegexp)

		// Wait for FFmpeg to exit (meaning OBS stopped streaming or client disconnected)
		err = cmd.Wait()
		monitorCancel()
		
		log.Printf("[Ingester] FFmpeg stream disconnected: %v. Restarting listener...", err)
		in.cleanupTempDir() // Clean up for the next session
		time.Sleep(1 * time.Second)
	}
}

// monitorPlaylist polls index.m3u8 for new chunks
func (in *Ingester) monitorPlaylist(ctx context.Context, playlistPath string, seqRegexp *regexp.Regexp) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	var lastQueuedSeq int64 = -1

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := os.Stat(playlistPath); os.IsNotExist(err) {
				continue
			}

			content, err := os.ReadFile(playlistPath)
			if err != nil {
				continue
			}

			lines := strings.Split(string(content), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}

				// The line represents the filename, e.g. chunk_0.ts
				matches := seqRegexp.FindStringSubmatch(line)
				if len(matches) != 2 {
					continue
				}

				seq, err := strconv.ParseInt(matches[1], 10, 64)
				if err != nil {
					continue
				}

				if seq > lastQueuedSeq {
					filePath := filepath.Join(in.cfg.TempDir, line)
					
					// Small delay to ensure OS has finished syncing the file contents
					time.Sleep(50 * time.Millisecond)
					
					log.Printf("[Ingester] Detected new chunk: %s (Seq: %d)", line, seq)
					in.chunkChan <- ChunkJob{
						Path:     filePath,
						SeqNum:   seq,
						Filename: line,
					}
					lastQueuedSeq = seq
				}
			}
		}
	}
}

func (in *Ingester) cleanupTempDir() {
	files, err := os.ReadDir(in.cfg.TempDir)
	if err != nil {
		return
	}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".ts") || strings.HasSuffix(f.Name(), ".m3u8") {
			_ = os.Remove(filepath.Join(in.cfg.TempDir, f.Name()))
		}
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}
