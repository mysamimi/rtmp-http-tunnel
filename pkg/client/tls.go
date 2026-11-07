package client

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

// newTLSConfig builds the TLS settings for talking to the server. Verification is
// on by default; a pinned fingerprint or private CA is needed for self-signed servers.
func newTLSConfig(cfg *common.ClientConfig) (*tls.Config, error) {
	if cfg.TLSPinSHA256 != "" {
		pin, err := common.ParsePin(cfg.TLSPinSHA256)
		if err != nil {
			return nil, fmt.Errorf("tls_pin_sha256: %w", err)
		}
		return pinnedTLSConfig(pin), nil
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.TLSCAFile != "" {
		pem, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("tls_ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls_ca_file %s contains no valid PEM certificates", cfg.TLSCAFile)
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.InsecureSkipVerify {
		tlsCfg.InsecureSkipVerify = true
	}
	return tlsCfg, nil
}

// pinnedTLSConfig accepts exactly one server certificate, identified by the SHA-256 of
// its DER encoding. The normal chain check is replaced by this comparison.
func pinnedTLSConfig(pin []byte) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		// Chain verification is replaced by the fingerprint check below.
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("server presented no certificate")
			}
			sum := sha256.Sum256(rawCerts[0])
			if !bytes.Equal(sum[:], pin) {
				return fmt.Errorf("server certificate fingerprint %x does not match tls_pin_sha256", sum[:])
			}
			return nil
		},
	}
}
