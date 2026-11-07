package server

import (
	"testing"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
)

func TestEffectiveInactiveTimeoutCoversBufferPlayout(t *testing.T) {
	tests := []struct {
		name                         string
		timeout, buffer, chunk, want int
	}{
		{"default fits", 120, 60, 2, 120},
		{"timeout shorter than buffer", 30, 60, 2, 68},
		{"long buffer", 120, 300, 2, 308},
		{"unset falls back, then clamps", 0, 60, 2, 120},
	}
	for _, tt := range tests {
		s := &Server{cfg: &common.ServerConfig{InactiveSessionTimeout: tt.timeout, BufferDuration: tt.buffer, ChunkDuration: tt.chunk}}
		if got := s.effectiveInactiveTimeout(); got != time.Duration(tt.want)*time.Second {
			t.Errorf("%s: effective timeout = %v, want %ds", tt.name, got, tt.want)
		}
	}
}
