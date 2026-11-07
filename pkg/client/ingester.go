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

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

type Ingester struct {
	cfg               *common.ClientConfig
	stream            common.StreamConfig
	chunkChan         chan<- ChunkJob
	seqMu             sync.Mutex
	nextUploadSeq     int64
	sequenceStatePath string
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
	in.sequenceStatePath = filepath.Join(in.stream.TempDir, ".rtmp-http-tunnel-next-seq")
	in.loadSequenceState()

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

	// Segment names carry a per-run prefix so chunks still waiting for upload are
	// never overwritten when FFmpeg restarts and begins numbering from zero again.
	var runID int

	isRTMP := strings.HasPrefix(strings.ToLower(in.stream.InputURL), "rtmp:") || strings.HasPrefix(strings.ToLower(in.stream.InputURL), "rtmps:")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		runID++
		segPrefix := fmt.Sprintf("r%d_", runID)

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
				dockerArgs = append(dockerArgs, "-p", spec.PublishArg())
			}
			if spec.NeedsHostAlias {
				dockerArgs = append(dockerArgs, "--add-host", DockerHostAlias+":host-gateway")
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
				"-hls_segment_filename", "/tmp_client/"+segPrefix+"chunk_%d.ts",
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
				"-hls_segment_filename", filepath.Join(in.stream.TempDir, segPrefix+"chunk_%d.ts"),
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
		drain := make(chan struct{})
		monitorDone := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(monitorDone)
			in.monitorPlaylistDrain(monitorCtx, drain, playlistPath, seqRegexp)
		}()

		// Wait for FFmpeg to exit (meaning OBS stopped streaming or client disconnected)
		err = cmd.Wait()

		// FFmpeg finalizes the last segment on exit: let the monitor queue it before stopping.
		close(drain)
		<-monitorDone
		monitorCancel()

		log.Printf("[Ingester %s] FFmpeg process exited: %v. Restarting...", in.stream.StreamPath, err)
		// Only drop the playlist: chunk files still queued for upload are removed by
		// the uploader once sent or expired.
		_ = os.Remove(playlistPath)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}

// monitorPlaylist polls index.m3u8 for new chunks
func (in *Ingester) monitorPlaylist(ctx context.Context, playlistPath string, seqRegexp *regexp.Regexp) {
	in.monitorPlaylistDrain(ctx, nil, playlistPath, seqRegexp)
}

// monitorPlaylistDrain is monitorPlaylist with an extra signal: when drain is closed
// the playlist is scanned one last time (to pick up the final segment) before returning.
func (in *Ingester) monitorPlaylistDrain(ctx context.Context, drain <-chan struct{}, playlistPath string, seqRegexp *regexp.Regexp) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	var lastQueuedSeq int64 = -1

	for {
		select {
		case <-ctx.Done():
			return
		case <-drain:
			in.scanPlaylist(ctx, playlistPath, seqRegexp, &lastQueuedSeq)
			return
		case <-ticker.C:
			if !in.scanPlaylist(ctx, playlistPath, seqRegexp, &lastQueuedSeq) {
				return
			}
		}
	}
}

// scanPlaylist queues every chunk listed in the playlist that has not been queued yet.
// It returns false when ctx was cancelled.
func (in *Ingester) scanPlaylist(ctx context.Context, playlistPath string, seqRegexp *regexp.Regexp, lastQueuedSeq *int64) bool {
	content, err := os.ReadFile(playlistPath)
	if err != nil {
		return true
	}

	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// The line represents the filename, e.g. r1_chunk_0.ts
		matches := seqRegexp.FindStringSubmatch(line)
		if len(matches) != 2 {
			continue
		}

		hlsSeq, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			continue
		}

		if hlsSeq > *lastQueuedSeq {
			filePath := filepath.Join(in.stream.TempDir, line)
			uploadSeq := in.nextSequence()
			uploadFilename := fmt.Sprintf("chunk_%d.ts", uploadSeq)

			// Small delay to ensure OS has finished syncing the file contents
			time.Sleep(50 * time.Millisecond)

			log.Printf("[Ingester %s] Detected new chunk: %s (HLS Seq: %d, Upload Seq: %d)", in.stream.StreamPath, line, hlsSeq, uploadSeq)
			select {
			case <-ctx.Done():
				return false
			case in.chunkChan <- ChunkJob{
				Path:      filePath,
				SeqNum:    uploadSeq,
				Filename:  uploadFilename,
				CreatedAt: time.Now(),
			}:
			}
			*lastQueuedSeq = hlsSeq
		}
	}
	return true
}

func (in *Ingester) nextSequence() int64 {
	in.seqMu.Lock()
	defer in.seqMu.Unlock()

	seq := in.nextUploadSeq
	in.nextUploadSeq++
	in.persistSequenceStateLocked()
	return seq
}

func (in *Ingester) loadSequenceState() {
	in.seqMu.Lock()
	defer in.seqMu.Unlock()

	data, err := os.ReadFile(in.sequenceStatePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[Ingester %s] Failed to read sequence state %s: %v", in.stream.StreamPath, in.sequenceStatePath, err)
		}
		return
	}

	nextSeq, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || nextSeq < 0 {
		log.Printf("[Ingester %s] Ignoring invalid sequence state %s", in.stream.StreamPath, in.sequenceStatePath)
		return
	}

	in.nextUploadSeq = nextSeq
	log.Printf("[Ingester %s] Resuming upload sequence from %d", in.stream.StreamPath, in.nextUploadSeq)
}

func (in *Ingester) persistSequenceStateLocked() {
	if in.sequenceStatePath == "" {
		return
	}
	data := []byte(strconv.FormatInt(in.nextUploadSeq, 10))
	if err := os.WriteFile(in.sequenceStatePath, data, 0644); err != nil {
		log.Printf("[Ingester %s] Failed to persist sequence state %s: %v", in.stream.StreamPath, in.sequenceStatePath, err)
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
