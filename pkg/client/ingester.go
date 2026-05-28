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

// Start runs the FFmpeg listener or pull stream and HLS segmenter loop.
func (in *Ingester) Start(ctx context.Context) error {
	// Create temp directory if it doesn't exist
	if err := os.MkdirAll(in.stream.TempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	// We clean up any old chunk files in the temp directory at startup
	in.cleanupTempDir()

	playlistPath := filepath.Join(in.stream.TempDir, "index.m3u8")

	spec, err := ParseInputURL(in.stream.InputURL)
	if err != nil {
		return fmt.Errorf("invalid input_url: %w", err)
	}

	log.Printf("[Ingester %s] Starting pipeline for input: %s (Listener: %v)", in.stream.StreamPath, in.stream.InputURL, spec.IsListener)
	log.Printf("[Ingester %s] Storing temporary HLS chunks in %s", in.stream.StreamPath, in.stream.TempDir)

	// Regexp to extract sequence number from chunk file name (e.g., chunk_12.ts)
	seqRegexp := regexp.MustCompile(`chunk_(\d+)\.ts`)

	var containerName string
	if spec.IsListener && spec.ListenPort != "" {
		containerName = fmt.Sprintf("rtmp-http-tunnel-client-%s-%s", spec.ListenPort, in.stream.StreamPath)
	} else {
		containerName = fmt.Sprintf("rtmp-http-tunnel-client-pull-%s", in.stream.StreamPath)
	}

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

	isRTMP := strings.HasPrefix(strings.ToLower(in.stream.InputURL), "rtmp:") || strings.HasPrefix(strings.ToLower(in.stream.InputURL), "rtmps:")

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

			dockerArgs := []string{
				"run", "--rm",
				"--name", containerName,
			}
			if spec.IsListener && spec.ListenPort != "" {
				dockerArgs = append(dockerArgs, "-p", fmt.Sprintf("%s:%s", spec.ListenPort, spec.ListenPort))
			}
			dockerArgs = append(dockerArgs,
				"-v", fmt.Sprintf("%s:/tmp_client", absTempDir),
				image,
				"-loglevel", "info",
			)
			if spec.IsListener && isRTMP {
				dockerArgs = append(dockerArgs, "-listen", "1")
			}
			dockerArgs = append(dockerArgs,
				"-i", spec.DockerInputURL,
				"-c", "copy",
				"-f", "hls",
				"-hls_time", strconv.Itoa(in.cfg.ChunkDuration),
				"-hls_list_size", "9999999",
				"-hls_segment_filename", "/tmp_client/chunk_%d.ts",
				"/tmp_client/index.m3u8",
			)

			cmd = exec.CommandContext(ctx, "docker", dockerArgs...)
			log.Printf("[Ingester %s] Running FFmpeg in Docker (%s), input URL: %s", in.stream.StreamPath, image, spec.DockerInputURL)
		} else {
			nativeArgs := []string{
				"-loglevel", "info",
			}
			if spec.IsListener && isRTMP {
				nativeArgs = append(nativeArgs, "-listen", "1")
			}
			nativeArgs = append(nativeArgs,
				"-i", spec.FFmpegInputURL,
				"-c", "copy",
				"-f", "hls",
				"-hls_time", strconv.Itoa(in.cfg.ChunkDuration),
				"-hls_list_size", "9999999",
				"-hls_segment_filename", filepath.Join(in.stream.TempDir, "chunk_%d.ts"),
				playlistPath,
			)

			cmd = exec.CommandContext(ctx, "ffmpeg", nativeArgs...)
			log.Printf("[Ingester %s] Running native FFmpeg, input URL: %s", in.stream.StreamPath, spec.FFmpegInputURL)
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

		if spec.IsListener {
			log.Printf("[Ingester %s] FFmpeg listener started, waiting for stream connection...", in.stream.StreamPath)
		} else {
			log.Printf("[Ingester %s] FFmpeg pull connection started...", in.stream.StreamPath)
		}

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

		log.Printf("[Ingester %s] FFmpeg process exited: %v. Restarting...", in.stream.StreamPath, err)
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
