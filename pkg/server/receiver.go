package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"rtmp-http-tunnel/pkg/common"
)

type Receiver struct {
	cfg    *common.ServerConfig
	server *Server
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
		Addr:    r.cfg.ListenAddr,
		Handler: mux,
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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
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
	authHeader := req.Header.Get("Authorization")
	if r.cfg.AuthToken != "" {
		expectedHeader := "Bearer " + r.cfg.AuthToken
		if authHeader != expectedHeader {
			log.Printf("[Receiver] Unauthorized request to %s from %s", req.URL.Path, req.RemoteAddr)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
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
	filename := req.Header.Get("X-Filename")

	if seqStr == "" || filename == "" {
		http.Error(w, "Missing metadata headers", http.StatusBadRequest)
		return
	}

	seqNum, err := strconv.ParseInt(seqStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid sequence number", http.StatusBadRequest)
		return
	}

	// Prevent directory traversal attacks by taking only the base name of the file
	filename = filepath.Base(filename)
	if !strings.HasSuffix(filename, ".ts") {
		http.Error(w, "Invalid file format", http.StatusBadRequest)
		return
	}

	// 2. Read Body (Limit reader size to 50MB to prevent OOM)
	bodyBytes, err := io.ReadAll(io.LimitReader(req.Body, 50*1024*1024))
	if err != nil {
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

	// 5. Save Chunk to Temp Directory
	filePath := filepath.Join(session.TempDir, filename)
	err = os.WriteFile(filePath, decryptedData, 0644)
	if err != nil {
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
