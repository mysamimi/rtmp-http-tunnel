package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

type Streamer struct {
	cfg           *common.ServerConfig
	targetRTMPURL string
	buffer        *Buffer
	// ContainerName names the Docker container so it can be removed explicitly:
	// killing the docker CLI process does not stop the container it started.
	ContainerName string
}

func NewStreamer(cfg *common.ServerConfig, targetRTMPURL string, buffer *Buffer) *Streamer {
	return &Streamer{
		cfg:           cfg,
		targetRTMPURL: targetRTMPURL,
		buffer:        buffer,
	}
}

// Start launches the restreamer loop. It pulls chunks from the buffer and writes them to FFmpeg.
func (s *Streamer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		log.Printf("[Streamer] Waiting for buffer before connecting output to: %s", s.targetRTMPURL)
		if err := s.buffer.WaitUntilReady(ctx); err != nil {
			return err
		}
		log.Printf("[Streamer] Starting FFmpeg restreamer output to: %s", s.targetRTMPURL)

		// Start FFmpeg to read from stdin (-) and output to RTMP.
		var cmd *exec.Cmd
		if s.cfg.UseDocker {
			image := s.cfg.DockerImage
			if image == "" {
				image = "linuxserver/ffmpeg"
			}
			// Make sure no earlier container still holds the target connection.
			s.removeContainer()
			cmd = exec.CommandContext(ctx, "docker", s.dockerRunArgs(image)...)
			log.Printf("[Streamer] Running FFmpeg in Docker (%s)...", image)
		} else {
			cmd = exec.CommandContext(ctx, "ffmpeg",
				"-loglevel", "info",
				"-nostdin",
				"-re",
				"-f", "mpegts",
				"-i", "-",
				"-c", "copy",
				"-f", "flv",
				s.targetRTMPURL,
			)
			log.Println("[Streamer] Running native FFmpeg...")
		}

		stdinPipe, err := cmd.StdinPipe()
		if err != nil {
			log.Printf("[Streamer] Failed to open FFmpeg stdin pipe: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// Optional: pipe ffmpeg output to server logs for debugging
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			log.Printf("[Streamer] Failed to start FFmpeg: %v. Is ffmpeg installed?", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Println("[Streamer] FFmpeg output process started, feeding buffered chunks...")

		// Feed chunks loop
		streamErr := s.feedChunks(ctx, stdinPipe)

		// If feedChunks exits, we close the stdin pipe and wait for FFmpeg to finish
		_ = stdinPipe.Close()

		// Kill the process if it's still running (for Docker this only kills the CLI;
		// the container itself is removed below)
		_ = cmd.Process.Kill()

		_ = cmd.Wait()
		s.removeContainer()

		// Keep the buffered chunks in every case: a dropped connection to the
		// target must not discard the jitter buffer, so streaming resumes
		// from the next unsent chunk as soon as FFmpeg reconnects.
		if errors.Is(streamErr, ErrBufferDrained) {
			log.Printf("[Streamer] FFmpeg output process exited because buffer drained: %v. Waiting for quick rebuffer...", streamErr)
		} else {
			log.Printf("[Streamer] FFmpeg output process exited: %v. Reconnecting with buffered chunks preserved...", streamErr)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Streamer) feedChunks(ctx context.Context, writer io.WriteCloser) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Pop next chunk in sequence (blocks if empty or buffering)
		filePath, seq, err := s.buffer.Pop(ctx)
		if err != nil {
			// This could be a timeout skip or a context cancellation
			log.Printf("[Streamer] Buffer pop error: %v", err)
			if errors.Is(err, ErrBufferDrained) {
				return err
			}
			continue
		}

		// Read chunk data from disk
		data, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("[Streamer] Failed to read chunk file %s: %v", filePath, err)
			_ = os.Remove(filePath)
			continue
		}

		// Write data to FFmpeg stdin
		_, err = writer.Write(data)

		// Delete file as we have written it to FFmpeg pipe
		_ = os.Remove(filePath)

		if err != nil {
			return fmt.Errorf("failed writing chunk %d to ffmpeg stdin (likely broken pipe): %w", seq, err)
		}

		log.Printf("[Streamer] Wrote chunk Seq %d to FFmpeg", seq)
	}
}

// dockerRunArgs builds the arguments of `docker run` for the restreaming FFmpeg.
func (s *Streamer) dockerRunArgs(image string) []string {
	args := []string{"run", "--rm", "-i"}
	if s.ContainerName != "" {
		args = append(args, "--name", s.ContainerName)
	}
	return append(args,
		image,
		"-loglevel", "info",
		"-nostdin",
		"-re",
		"-f", "mpegts",
		"-i", "-",
		"-c", "copy",
		"-f", "flv",
		s.targetRTMPURL,
	)
}

// removeContainer force-removes the named container, ignoring "no such container".
func (s *Streamer) removeContainer() {
	if !s.cfg.UseDocker || s.ContainerName == "" {
		return
	}
	_ = exec.Command("docker", "rm", "-f", s.ContainerName).Run()
}
