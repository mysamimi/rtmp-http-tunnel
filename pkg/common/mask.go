package common

import (
	"bytes"
	"io"
	"net/url"
	"strings"
	"sync"
)

// MaskURL hides the secret parts of a streaming URL (the stream key in the last path
// segment, any query string and credentials) so it can be logged safely.
// rtmp://a.rtmp.youtube.com/live2/abcd-efgh -> rtmp://a.rtmp.youtube.com/live2/ab***
func MaskURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "***"
	}
	masked := u.Scheme + "://" + u.Host
	if u.User != nil {
		masked = u.Scheme + "://***@" + u.Host
	}
	path := strings.TrimRight(u.Path, "/")
	if i := strings.LastIndex(path, "/"); i >= 0 {
		masked += path[:i+1] + maskSecret(path[i+1:])
	}
	if u.RawQuery != "" {
		masked += "?***"
	}
	return masked
}

func maskSecret(s string) string {
	if len(s) <= 4 {
		return "***"
	}
	return s[:2] + "***"
}

// MaskingWriter forwards complete lines to w with every secret replaced by its
// masked form. Lines are buffered so a secret split across writes is still caught.
type MaskingWriter struct {
	mu   sync.Mutex
	w    io.Writer
	repl [][2][]byte
	buf  []byte
}

// NewMaskingWriter replaces each full URL in urls, and the secret key found in it,
// with the masked URL / key.
func NewMaskingWriter(w io.Writer, urls ...string) *MaskingWriter {
	m := &MaskingWriter{w: w}
	for _, raw := range urls {
		m.repl = append(m.repl, [2][]byte{[]byte(raw), []byte(MaskURL(raw))})
		if u, err := url.Parse(raw); err == nil {
			path := strings.TrimRight(u.Path, "/")
			if i := strings.LastIndex(path, "/"); i >= 0 && len(path[i+1:]) > 4 {
				key := path[i+1:]
				m.repl = append(m.repl, [2][]byte{[]byte(key), []byte(maskSecret(key))})
			}
		}
	}
	return m
}

func (m *MaskingWriter) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buf = append(m.buf, p...)
	for {
		i := bytes.IndexAny(m.buf, "\r\n")
		if i < 0 {
			break
		}
		if _, err := m.w.Write(m.mask(m.buf[:i+1])); err != nil {
			return len(p), err
		}
		m.buf = m.buf[i+1:]
	}
	return len(p), nil
}

// Flush writes any buffered partial line.
func (m *MaskingWriter) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.buf) == 0 {
		return nil
	}
	_, err := m.w.Write(m.mask(m.buf))
	m.buf = nil
	return err
}

func (m *MaskingWriter) mask(line []byte) []byte {
	out := line
	for _, r := range m.repl {
		out = bytes.ReplaceAll(out, r[0], r[1])
	}
	return out
}
