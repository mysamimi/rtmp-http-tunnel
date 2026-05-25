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

	// Extract port from LocalRTMPAddr
	port := "1935"
	parts := strings.Split(in.cfg.LocalRTMPAddr, ":")
	if len(parts) > 1 {
		portPart := parts[len(parts)-1]
		if slashIdx := strings.Index(portPart, "/"); slashIdx != -1 {
			port = portPart[:slashIdx]
		} else {
			port = portPart
		}
	}

	containerName := fmt.Sprintf("rtmp-http-tunnel-client-%s", port)

	// Clean up any leftover container on startup
	if in.cfg.UseDocker {
		log.Printf("[Ingester] Pre-cleaning Docker container %s...", containerName)
		_ = exec.Command("docker", "rm", "-f", containerName).Run()
	}

	// Register graceful cleanup for the Docker container when Ingester exits
	defer func() {
		if in.cfg.UseDocker {
			log.Printf("[Ingester] Cleaning up Docker container %s...", containerName)
			// Using background context to ensure this runs even if the parent context is cancelled
			_ = exec.Command("docker", "rm", "-f", containerName).Run()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var cmd *exec.Cmd
		if in.cfg.UseDocker {
			absTempDir, err := filepath.Abs(in.cfg.TempDir)
			if err != nil {
				log.Printf("[Ingester] Failed to resolve absolute path of temp dir: %v", err)
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
			log.Printf("[Ingester] Running FFmpeg in Docker (%s), mapping port %s -> 1935...", image, port)
		} else {
			rtmpURL := fmt.Sprintf("rtmp://%s", in.cfg.LocalRTMPAddr)
			if !strings.Contains(in.cfg.LocalRTMPAddr, "/") {
				rtmpURL = fmt.Sprintf("rtmp://%s/live/app", in.cfg.LocalRTMPAddr)
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
				"-hls_segment_filename", filepath.Join(in.cfg.TempDir, "chunk_%d.ts"),
				playlistPath,
			)
			log.Printf("[Ingester] Running native FFmpeg, listening on %s...", rtmpURL)
		}

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
					// Log ffmpeg detailed debug logs
					log.Printf("[FFmpeg] %s", string(buf[:n]))
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
