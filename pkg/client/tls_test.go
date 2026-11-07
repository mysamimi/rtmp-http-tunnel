package client

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

func tlsClientFor(t *testing.T, cfg *common.ClientConfig) *http.Client {
	t.Helper()
	tlsCfg, err := newTLSConfig(cfg)
	if err != nil {
		t.Fatalf("newTLSConfig: %v", err)
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}
}

func newTLSServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	t.Cleanup(srv.Close)
	sum := sha256.Sum256(srv.Certificate().Raw)
	return srv, hex.EncodeToString(sum[:])
}

func TestTLSSelfSignedRejectedByDefault(t *testing.T) {
	srv, _ := newTLSServer(t)
	if _, err := tlsClientFor(t, &common.ClientConfig{}).Get(srv.URL); err == nil {
		t.Fatal("self-signed certificate accepted without pin or opt-in")
	}
}

func TestTLSPinAcceptsMatchingCertificate(t *testing.T) {
	srv, pin := newTLSServer(t)
	resp, err := tlsClientFor(t, &common.ClientConfig{TLSPinSHA256: pin}).Get(srv.URL)
	if err != nil {
		t.Fatalf("pinned certificate rejected: %v", err)
	}
	resp.Body.Close()
}

func TestTLSPinRejectsOtherCertificate(t *testing.T) {
	srv, _ := newTLSServer(t)
	// httptest servers share one built-in certificate, so use an unrelated fingerprint.
	otherPin := hex.EncodeToString(make([]byte, 32))
	if _, err := tlsClientFor(t, &common.ClientConfig{TLSPinSHA256: otherPin}).Get(srv.URL); err == nil {
		t.Fatal("certificate with a different fingerprint was accepted")
	}
}

func TestTLSInsecureOptIn(t *testing.T) {
	srv, _ := newTLSServer(t)
	resp, err := tlsClientFor(t, &common.ClientConfig{InsecureSkipVerify: true}).Get(srv.URL)
	if err != nil {
		t.Fatalf("insecure_skip_verify not honored: %v", err)
	}
	resp.Body.Close()
}

func TestTLSBadCAFile(t *testing.T) {
	if _, err := newTLSConfig(&common.ClientConfig{TLSCAFile: "/nonexistent/ca.pem"}); err == nil {
		t.Fatal("missing CA file accepted")
	}
}
