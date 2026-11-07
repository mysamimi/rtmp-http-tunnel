package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

const maxUploadBodySize = 50 * 1024 * 1024

const unauthorizedLogInterval = 10 * time.Second

type Receiver struct {
	cfg    *common.ServerConfig
	server *Server

	logMu              sync.Mutex
	lastAuthLog        time.Time
	suppressedAuthLogs int
}

func NewReceiver(cfg *common.ServerConfig, server *Server) *Receiver {
	return &Receiver{
		cfg:    cfg,
		server: server,
	}
}

// Start launches the HTTP/HTTPS receiver server
func (r *Receiver) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/s/", r.handleStreamRequest)
	mux.HandleFunc("/health", r.handleGlobalHealth)

	server := &http.Server{
		Addr:              r.cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Set up graceful shutdown listener
	go func() {
		<-ctx.Done()
		log.Println("[Receiver] Shutting down HTTP server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("[Receiver] HTTP server Shutdown error: %v", err)
		}
	}()

	// Determine if running HTTP or HTTPS
	var err error
	if r.cfg.TLSCertFile != "" && r.cfg.TLSKeyFile != "" {
		log.Printf("[Receiver] Starting HTTPS server on %s", r.cfg.ListenAddr)
		err = server.ListenAndServeTLS(r.cfg.TLSCertFile, r.cfg.TLSKeyFile)
	} else {
		log.Printf("[Receiver] Starting HTTP server on %s (Warning: Not encrypted at network layer)", r.cfg.ListenAddr)
		err = server.ListenAndServe()
	}

	if err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (r *Receiver) handleGlobalHealth(w http.ResponseWriter, req *http.Request) {
	if !r.authorized(req) {
		r.rejectUnauthorized(w, req)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// authorized checks the bearer token in constant time.
func (r *Receiver) authorized(req *http.Request) bool {
	expected := []byte("Bearer " + r.cfg.AuthToken)
	got := []byte(req.Header.Get("Authorization"))
	return subtle.ConstantTimeCompare(got, expected) == 1
}

// rejectUnauthorized answers exactly like an unknown URL (a plain 404) so that an
// active prober cannot tell this service apart from an ordinary web server.
// Failures are logged at most once every few seconds to avoid log flooding.
func (r *Receiver) rejectUnauthorized(w http.ResponseWriter, req *http.Request) {
	r.logMu.Lock()
	now := time.Now()
	if now.Sub(r.lastAuthLog) >= unauthorizedLogInterval {
		log.Printf("[Receiver] Unauthorized request to %s from %s (%d similar suppressed)", req.URL.Path, req.RemoteAddr, r.suppressedAuthLogs)
		r.lastAuthLog = now
		r.suppressedAuthLogs = 0
	} else {
		r.suppressedAuthLogs++
	}
	r.logMu.Unlock()
	http.NotFound(w, req)
}

func (r *Receiver) handleStreamRequest(w http.ResponseWriter, req *http.Request) {
	// Parse path /s/{stream_path}/{action}
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "s" {
		http.NotFound(w, req)
		return
	}
	streamPath := parts[1]
	action := parts[2]

	// 1. Verify Authentication Token
	if !r.authorized(req) {
		r.rejectUnauthorized(w, req)
		return
	}

	// 2. Resolve Stream Session
	session := r.server.GetSession(streamPath)
	if session == nil {
		log.Printf("[Receiver] Stream session not found for path: %s", streamPath)
		http.Error(w, "Stream not found", http.StatusNotFound)
		return
	}

	switch action {
	case "health":
		if req.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.handleHealth(w, req, session)
	case "upload":
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.handleUpload(w, req, session)
	default:
		http.NotFound(w, req)
	}
}

func (r *Receiver) handleHealth(w http.ResponseWriter, req *http.Request, session *StreamSession) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := fmt.Sprintf(`{"status":"ok","buffer_duration":%d,"chunk_duration":%d}`,
		r.cfg.BufferDuration, r.cfg.ChunkDuration)
	_, _ = w.Write([]byte(resp))
}

func (r *Receiver) handleUpload(w http.ResponseWriter, req *http.Request, session *StreamSession) {
	// 1. Extract Metadata Headers
	seqStr := req.Header.Get("X-Sequence-Number")

	if seqStr == "" {
		http.Error(w, "Missing metadata headers", http.StatusBadRequest)
		return
	}

	seqNum, err := strconv.ParseInt(seqStr, 10, 64)
	if err != nil || seqNum < 0 {
		http.Error(w, "Invalid sequence number", http.StatusBadRequest)
		return
	}

	// The on-disk name is derived from the sequence number, never from client input,
	// so duplicate or retried uploads map to the same file and cannot escape TempDir.
	filename := fmt.Sprintf("chunk_%d.ts", seqNum)

	// 2. Read Body (hard limit to prevent OOM and truncated/corrupt chunks)
	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxUploadBodySize))
	if err != nil {
		if isRequestTooLarge(err) {
			log.Printf("[Receiver] Rejected oversized chunk for %s: %v", session.Path, err)
			http.Error(w, "Chunk too large", http.StatusRequestEntityTooLarge)
			return
		}
		log.Printf("[Receiver] Failed to read request body for %s: %v", session.Path, err)
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	defer req.Body.Close()

	// 3. Decrypt / De-obfuscate Chunk
	decryptedData := common.XORApply(bodyBytes, r.cfg.ObfuscationKey)

	// Verify MPEG-TS sync byte (0x47 / 'G') to detect key mismatch or corruption
	if len(decryptedData) > 0 && decryptedData[0] != 0x47 {
		log.Printf("[Receiver] WARNING: Decrypted chunk %s does not start with MPEG-TS sync byte (0x47, 'G'). Your obfuscation_key might be mismatched between client and server!", filename)
	}

	// 4. Ensure streamer is running for this session
	r.server.StartSessionStreamer(r.server.ctx, session)

	// A retried upload of a chunk we already hold (or already played) must not touch
	// the file the streamer may be reading.
	if !session.Buffer.ShouldAccept(seqNum) {
		log.Printf("[Receiver] Ignoring duplicate or late chunk %s for path: %s", filename, session.Path)
		session.Mu.Lock()
		session.LastActivity = time.Now()
		session.Mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Chunk ignored"))
		return
	}

	// 5. Save Chunk to Temp Directory (write + rename so readers never see partial data)
	filePath := filepath.Join(session.TempDir, filename)
	partPath := filepath.Join(session.TempDir, "."+filename+".part")
	if err = os.WriteFile(partPath, decryptedData, 0644); err == nil {
		err = os.Rename(partPath, filePath)
	}
	if err != nil {
		_ = os.Remove(partPath)
		log.Printf("[Receiver] Failed to save chunk to disk for %s: %v", session.Path, err)
		http.Error(w, "Failed to write file", http.StatusInternalServerError)
		return
	}

	log.Printf("[Receiver] Successfully received chunk %s (Seq: %d, size: %d bytes) for path: %s", filename, seqNum, len(decryptedData), session.Path)

	// 6. Queue in Buffer
	session.Buffer.Add(seqNum, filePath)

	// 7. Update activity time
	session.Mu.Lock()
	session.LastActivity = time.Now()
	session.Mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Chunk received"))
}

func isRequestTooLarge(err error) bool {
	if err == nil {
		return false
	}
	var maxBytesErr *http.MaxBytesError
	return errors.As(err, &maxBytesErr)
}
