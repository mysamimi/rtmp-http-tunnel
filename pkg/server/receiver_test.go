package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

func newTestReceiver(t *testing.T) (*Receiver, *StreamSession) {
	t.Helper()
	cfg := &common.ServerConfig{BufferDuration: 100, ChunkDuration: 2, ObfuscationKey: "k"}
	dir := t.TempDir()
	buf := NewBuffer(cfg, dir)
	session := &StreamSession{Buffer: buf, Streamer: NewStreamer(cfg, "rtmp://x", buf), Path: "p", TempDir: dir}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv := &Server{cfg: cfg, ctx: ctx}
	return NewReceiver(cfg, srv), session
}

func upload(r *Receiver, session *StreamSession, seq, filename string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/s/p/upload", bytes.NewReader(common.XORApply(body, "k")))
	req.Header.Set("X-Sequence-Number", seq)
	if filename != "" {
		req.Header.Set("X-Filename", filename)
	}
	w := httptest.NewRecorder()
	r.handleUpload(w, req, session)
	return w
}

func TestHandleUploadIgnoresClientFilename(t *testing.T) {
	r, session := newTestReceiver(t)
	w := upload(r, session, "7", "../../evil.ts", []byte{0x47, 1, 2})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if _, err := os.Stat(filepath.Join(session.TempDir, "chunk_7.ts")); err != nil {
		t.Fatalf("chunk not stored under sequence-derived name: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(session.TempDir), "evil.ts")); !os.IsNotExist(err) {
		t.Fatal("client-supplied filename was used")
	}
}

func TestHandleUploadDuplicateDoesNotRewriteFile(t *testing.T) {
	r, session := newTestReceiver(t)
	upload(r, session, "3", "", []byte{0x47, 'a'})
	w := upload(r, session, "3", "", []byte{0x47, 'b'})
	if w.Code != http.StatusOK {
		t.Fatalf("duplicate status = %d, want 200", w.Code)
	}
	data, _ := os.ReadFile(filepath.Join(session.TempDir, "chunk_3.ts"))
	if !bytes.Equal(data, []byte{0x47, 'a'}) {
		t.Fatalf("duplicate upload overwrote buffered chunk: %v", data)
	}
	if w := upload(r, session, "-1", "", []byte{0x47}); w.Code != http.StatusBadRequest {
		t.Fatalf("negative sequence status = %d, want 400", w.Code)
	}
}

func TestUnauthenticatedRequestsLookLikeUnknownURLs(t *testing.T) {
	r, session := newTestReceiver(t)
	r.cfg.AuthToken = "secret"
	r.server.sessions = map[string]*StreamSession{"p": session}

	mux := http.NewServeMux()
	mux.HandleFunc("/s/", r.handleStreamRequest)
	mux.HandleFunc("/health", r.handleGlobalHealth)

	do := func(method, path, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w
	}

	baseline := do(http.MethodGet, "/nothing-here", "")
	for _, c := range []struct{ method, path, token string }{
		{http.MethodGet, "/health", ""},
		{http.MethodGet, "/health", "wrong"},
		{http.MethodGet, "/s/p/health", ""},
		{http.MethodPost, "/s/p/upload", "wrong"},
		{http.MethodGet, "/s/unknown/health", ""},
		{http.MethodGet, "/s/p", ""},
	} {
		w := do(c.method, c.path, c.token)
		if w.Code != baseline.Code || w.Body.String() != baseline.Body.String() {
			t.Errorf("%s %s (token %q) = %d %q, want same as unknown URL (%d %q)",
				c.method, c.path, c.token, w.Code, w.Body.String(), baseline.Code, baseline.Body.String())
		}
	}

	if w := do(http.MethodGet, "/s/p/health", "secret"); w.Code != http.StatusOK {
		t.Errorf("authorized health = %d, want 200", w.Code)
	}
	if w := do(http.MethodGet, "/health", "secret"); w.Code != http.StatusOK {
		t.Errorf("authorized global health = %d, want 200", w.Code)
	}
}
