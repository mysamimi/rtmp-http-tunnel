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
	buffer *Buffer
}

func NewReceiver(cfg *common.ServerConfig, buffer *Buffer) *Receiver {
	return &Receiver{
		cfg:    cfg,
		buffer: buffer,
	}
}

// Start launches the HTTP/HTTPS receiver server
func (r *Receiver) Start(ctx context.Context) error {
	// Ensure temp directory exists
	if err := os.MkdirAll(r.cfg.TempDir, 0755); err != nil {
		return fmt.Errorf("failed to create server temp dir: %w", err)
	}

	// Clean up any old files from previous run
	r.cleanupTempDir()

	mux := http.NewServeMux()
	mux.HandleFunc("/upload", r.handleUpload)
	mux.HandleFunc("/health", r.handleHealth)

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

func (r *Receiver) handleHealth(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (r *Receiver) handleUpload(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. Verify Authentication Token
	authHeader := req.Header.Get("Authorization")
	if r.cfg.AuthToken != "" {
		expectedHeader := "Bearer " + r.cfg.AuthToken
		if authHeader != expectedHeader {
			log.Printf("[Receiver] Unauthorized upload attempt from %s", req.RemoteAddr)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// 2. Extract Metadata Headers
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

	// 3. Read Body
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		log.Printf("[Receiver] Failed to read request body: %v", err)
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	defer req.Body.Close()

	// 4. Decrypt / De-obfuscate Chunk
	decryptedData := common.XORApply(bodyBytes, r.cfg.ObfuscationKey)

	// Verify MPEG-TS sync byte (0x47 / 'G') to detect key mismatch or corruption
	if len(decryptedData) > 0 && decryptedData[0] != 0x47 {
		log.Printf("[Receiver] WARNING: Decrypted chunk %s does not start with MPEG-TS sync byte (0x47, 'G'). Your obfuscation_key might be mismatched between client and server!", filename)
	}

	// 5. Save Chunk to Temp Directory
	filePath := filepath.Join(r.cfg.TempDir, filename)
	err = os.WriteFile(filePath, decryptedData, 0644)
	if err != nil {
		log.Printf("[Receiver] Failed to save chunk to disk: %v", err)
		http.Error(w, "Failed to write file", http.StatusInternalServerError)
		return
	}

	log.Printf("[Receiver] Successfully received chunk %s (Seq: %d, size: %d bytes)", filename, seqNum, len(decryptedData))

	// 6. Queue in Buffer
	r.buffer.Add(seqNum, filePath)

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Chunk received"))
}

func (r *Receiver) cleanupTempDir() {
	files, err := os.ReadDir(r.cfg.TempDir)
	if err != nil {
		return
	}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".ts") {
			_ = os.Remove(filepath.Join(r.cfg.TempDir, f.Name()))
		}
	}
}
