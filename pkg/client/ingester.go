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
	"sync"
	"time"

	"rtmp-http-tunnel/pkg/common"
)

type Ingester struct {
	cfg       *common.ClientConfig
	stream    common.StreamConfig
	chunkChan chan<- ChunkJob
}

type ChunkJob struct {
	Path      string
	SeqNum    int64
	Filename  string
	CreatedAt time.Time
}

func NewIngester(cfg *common.ClientConfig, stream common.StreamConfig, chunkChan chan<- ChunkJob) *Ingester {
	return &Ingester{
		cfg:       cfg,
		stream:    stream,
		chunkChan: chunkChan,
	}
}

// Start runs the FFmpeg RTMP listener and HLS segmenter loop.
func (in *Ingester) Start(ctx context.Context) error {
	// Create temp directory if it doesn't exist
	if err := os.MkdirAll(in.stream.TempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	// We clean up any old chunk files in the temp directory at startup
	in.cleanupTempDir()

	playlistPath := filepath.Join(in.stream.TempDir, "index.m3u8")

	log.Printf("[Ingester %s] Starting local RTMP listener on rtmp://%s", in.stream.StreamPath, in.stream.LocalRTMPAddr)
	log.Printf("[Ingester %s] Storing temporary HLS chunks in %s", in.stream.StreamPath, in.stream.TempDir)

	// Regexp to extract sequence number from chunk file name (e.g., chunk_12.ts)
	seqRegexp := regexp.MustCompile(`chunk_(\d+)\.ts`)

	// Extract port from LocalRTMPAddr
	port := "1935"
	parts := strings.Split(in.stream.LocalRTMPAddr, ":")
	if len(parts) > 1 {
		portPart := parts[len(parts)-1]
		if slashIdx := strings.Index(portPart, "/"); slashIdx != -1 {
			port = portPart[:slashIdx]
		} else {
			port = portPart
		}
	}

	containerName := fmt.Sprintf("rtmp-http-tunnel-client-%s-%s", port, in.stream.StreamPath)

	// Clean up any leftover container on startup
	if in.cfg.UseDocker {
		log.Printf("[Ingester %s] Pre-cleaning Docker container %s...", in.stream.StreamPath, containerName)
		_ = exec.Command("docker", "rm", "-f", containerName).Run()
	}

	// Register graceful cleanup for the Docker container when Ingester exits
	defer func() {
		if in.cfg.UseDocker {
			log.Printf("[Ingester %s] Cleaning up Docker container %s...", in.stream.StreamPath, containerName)
			// Using background context to ensure this runs even if the parent context is cancelled
			_ = exec.Command("docker", "rm", "-f", containerName).Run()
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var cmd *exec.Cmd
		if in.cfg.UseDocker {
			absTempDir, err := filepath.Abs(in.stream.TempDir)
			if err != nil {
				log.Printf("[Ingester %s] Failed to resolve absolute path of temp dir: %v", in.stream.StreamPath, err)
				time.Sleep(2 * time.Second)
				continue
			}

			image := in.cfg.DockerImage
			if image == "" {
				image = "linuxserver/ffmpeg"
			}

			// We map host port to container's 1935 port where FFmpeg listens.
			cmd = exec.CommandContext(ctx, "docker", "run", "--rm",
				"--name", containerName,
				"-p", fmt.Sprintf("%s:1935", port),
				"-v", fmt.Sprintf("%s:/tmp_client", absTempDir),
				image,
				"-loglevel", "info",
				"-f", "flv",
				"-listen", "1",
				"-i", "rtmp://0.0.0.0:1935/live/app",
				"-c", "copy",
				"-f", "hls",
				"-hls_time", strconv.Itoa(in.cfg.ChunkDuration),
				"-hls_list_size", "9999999",
				"-hls_segment_filename", "/tmp_client/chunk_%d.ts",
				"/tmp_client/index.m3u8",
			)
			log.Printf("[Ingester %s] Running FFmpeg in Docker (%s), mapping port %s -> 1935...", in.stream.StreamPath, image, port)
		} else {
			rtmpURL := fmt.Sprintf("rtmp://%s", in.stream.LocalRTMPAddr)
			if !strings.Contains(in.stream.LocalRTMPAddr, "/") {
				rtmpURL = fmt.Sprintf("rtmp://%s/live/app", in.stream.LocalRTMPAddr)
			}

			cmd = exec.CommandContext(ctx, "ffmpeg",
				"-loglevel", "info",
				"-f", "flv",
				"-listen", "1",
				"-i", rtmpURL,
				"-c", "copy",
				"-f", "hls",
				"-hls_time", strconv.Itoa(in.cfg.ChunkDuration),
				"-hls_list_size", "9999999", // keep index long enough
				"-hls_segment_filename", filepath.Join(in.stream.TempDir, "chunk_%d.ts"),
				playlistPath,
			)
			log.Printf("[Ingester %s] Running native FFmpeg, listening on %s...", in.stream.StreamPath, rtmpURL)
		}

		// Capture stderr for logging if needed
		stderr, err := cmd.StderrPipe()
		if err != nil {
			log.Printf("[Ingester %s] Failed to pipe ffmpeg stderr: %v", in.stream.StreamPath, err)
			time.Sleep(2 * time.Second)
			continue
		}

		if err := cmd.Start(); err != nil {
			log.Printf("[Ingester %s] Failed to start FFmpeg: %v. Is ffmpeg installed?", in.stream.StreamPath, err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("[Ingester %s] FFmpeg listener started, waiting for stream connection...", in.stream.StreamPath)

		// Run stderr reader in goroutine to log stream state
		go func() {
			buf := make([]byte, 1024)
			for {
				n, err := stderr.Read(buf)
				if n > 0 {
					// Log ffmpeg detailed debug logs
					log.Printf("[FFmpeg-%s] %s", in.stream.StreamPath, string(buf[:n]))
				}
				if err != nil {
					break
				}
			}
		}()

		// Start monitoring the index.m3u8 playlist file while FFmpeg runs
		monitorCtx, monitorCancel := context.WithCancel(ctx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			in.monitorPlaylist(monitorCtx, playlistPath, seqRegexp)
		}()

		// Wait for FFmpeg to exit (meaning OBS stopped streaming or client disconnected)
		err = cmd.Wait()
		monitorCancel()

		log.Printf("[Ingester %s] FFmpeg stream disconnected: %v. Restarting listener...", in.stream.StreamPath, err)
		in.cleanupTempDir() // Clean up for the next session

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
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
					filePath := filepath.Join(in.stream.TempDir, line)

					// Small delay to ensure OS has finished syncing the file contents
					time.Sleep(50 * time.Millisecond)

					log.Printf("[Ingester %s] Detected new chunk: %s (Seq: %d)", in.stream.StreamPath, line, seq)
					select {
					case <-ctx.Done():
						return
					case in.chunkChan <- ChunkJob{
						Path:      filePath,
						SeqNum:    seq,
						Filename:  line,
						CreatedAt: time.Now(),
					}:
					}
					lastQueuedSeq = seq
				}
			}
		}
	}
}

func (in *Ingester) cleanupTempDir() {
	files, err := os.ReadDir(in.stream.TempDir)
	if err != nil {
		return
	}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".ts") || strings.HasSuffix(f.Name(), ".m3u8") {
			_ = os.Remove(filepath.Join(in.stream.TempDir, f.Name()))
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
