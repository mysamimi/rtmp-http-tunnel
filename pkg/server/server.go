package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

type StreamSession struct {
	Buffer        *Buffer
	Streamer      *Streamer
	CancelFunc    context.CancelFunc
	LastActivity  time.Time
	Mu            sync.Mutex
	Path          string
	TempDir       string
	TargetRTMPURL string
}

type Server struct {
	cfg      *common.ServerConfig
	sessions map[string]*StreamSession
	mu       sync.RWMutex
	ctx      context.Context
}

func NewServer(cfg *common.ServerConfig) *Server {
	return &Server{
		cfg:      cfg,
		sessions: make(map[string]*StreamSession),
	}
}

// Start launches the Server components
func (s *Server) Start(ctx context.Context) error {
	s.ctx = ctx

	s.mu.Lock()
	for _, streamCfg := range s.cfg.Streams {
		// Ensure temp directory exists
		if err := os.MkdirAll(streamCfg.TempDir, 0755); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("failed to create server temp dir for path %s: %w", streamCfg.Path, err)
		}

		// Create buffer and streamer for this session
		buf := NewBuffer(s.cfg, streamCfg.TempDir)
		str := NewStreamer(s.cfg, streamCfg.TargetRTMPURL, buf)

		session := &StreamSession{
			Buffer:        buf,
			Streamer:      str,
			Path:          streamCfg.Path,
			TempDir:       streamCfg.TempDir,
			TargetRTMPURL: streamCfg.TargetRTMPURL,
			LastActivity:  time.Now(),
		}

		// Clean up any old files from previous run
		session.Buffer.Reset()

		s.sessions[streamCfg.Path] = session
	}
	s.mu.Unlock()

	// Start background cleanup routine
	go s.startCleanupRoutine(ctx)

	receiver := NewReceiver(s.cfg, s)

	log.Println("[Server] Server orchestrator started successfully")

	// Start HTTP/HTTPS receiver (blocks until server stops or error occurs)
	return receiver.Start(ctx)
}

func (s *Server) GetSession(path string) *StreamSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[path]
}

func (s *Server) StartSessionStreamer(ctx context.Context, session *StreamSession) {
	session.Mu.Lock()
	defer session.Mu.Unlock()
	if session.CancelFunc == nil {
		sessionCtx, cancel := context.WithCancel(ctx)
		session.CancelFunc = cancel
		go func() {
			log.Printf("[Server] Starting streamer goroutine for stream path: %s", session.Path)
			if err := session.Streamer.Start(sessionCtx); err != nil && err != context.Canceled {
				log.Printf("[Server] Streamer for path %s exited with error: %v", session.Path, err)
			}
			session.Mu.Lock()
			session.CancelFunc = nil
			session.Mu.Unlock()
		}()
	}
}

func (s *Server) startCleanupRoutine(ctx context.Context) {
	timeout := s.cfg.InactiveSessionTimeout
	if timeout <= 0 {
		timeout = 120 // Default to 2 minutes
	}
	interval := time.Duration(timeout/2) * time.Second
	if interval < 2*time.Second {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.RLock()
			var sessions []*StreamSession
			for _, sess := range s.sessions {
				sessions = append(sessions, sess)
			}
			s.mu.RUnlock()

			for _, sess := range sessions {
				sess.Mu.Lock()
				if sess.CancelFunc != nil {
					if time.Since(sess.LastActivity) > time.Duration(timeout)*time.Second {
						log.Printf("[Server] Session %s is inactive (last activity: %v). Cleaning up...", sess.Path, sess.LastActivity)
						sess.CancelFunc()
						sess.CancelFunc = nil
						sess.Buffer.Cleanup()
					}
				}
				sess.Mu.Unlock()
			}
		}
	}
}
