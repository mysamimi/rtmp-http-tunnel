package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"time"

	"rtmp-http-tunnel/pkg/common"
)

type Streamer struct {
	cfg    *common.ServerConfig
	buffer *Buffer
}

func NewStreamer(cfg *common.ServerConfig, buffer *Buffer) *Streamer {
	return &Streamer{
		cfg:    cfg,
		buffer: buffer,
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

		log.Printf("[Streamer] Starting FFmpeg restreamer output to: %s", s.cfg.TargetRTMPURL)

		// Start FFmpeg to read from stdin (-) and output to RTMP.
		// -re: read input in real time
		// -i -: read from stdin
		// -c copy: copy video/audio codecs without transcoding (saves CPU)
		// -f flv: output format for RTMP ingest
		cmd := exec.CommandContext(ctx, "ffmpeg",
			"-loglevel", "info",
			"-re",
			"-i", "-",
			"-c", "copy",
			"-f", "flv",
			s.cfg.TargetRTMPURL,
		)

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

		log.Println("[Streamer] FFmpeg output process started, waiting for buffer to start feeding chunks...")

		// Feed chunks loop
		streamErr := s.feedChunks(ctx, stdinPipe)
		
		// If feedChunks exits, we close the stdin pipe and wait for FFmpeg to finish
		_ = stdinPipe.Close()
		
		// Kill the process if it's still running
		_ = cmd.Process.Kill()
		
		_ = cmd.Wait()

		log.Printf("[Streamer] FFmpeg output process exited: %v. Resetting buffer and restarting streamer...", streamErr)
		s.buffer.Reset() // Clear queue so we start clean next time
		time.Sleep(2 * time.Second)
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
