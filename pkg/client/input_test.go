package client

import (
	"strings"
	"testing"
)

func TestParseInputURL(t *testing.T) {
	tests := []struct {
		name          string
		rawURL        string
		wantListen    bool
		wantPort      string
		wantFFmpegURL string
		wantDockerURL string
		expectError   bool
	}{
		{
			name:          "rtmp listener",
			rawURL:        "rtmp://0.0.0.0:1935/live/app",
			wantListen:    true,
			wantPort:      "1935",
			wantFFmpegURL: "rtmp://0.0.0.0:1935/live/app",
			wantDockerURL: "rtmp://0.0.0.0:1935/live/app",
			expectError:   false,
		},
		{
			name:          "rtmp listener short host",
			rawURL:        "rtmp://0:1935",
			wantListen:    true,
			wantPort:      "1935",
			wantFFmpegURL: "rtmp://0:1935",
			wantDockerURL: "rtmp://0:1935",
			expectError:   false,
		},
		{
			name:          "rtmp pull",
			rawURL:        "rtmp://192.168.1.12:1935/live/app",
			wantListen:    false,
			wantPort:      "1935",
			wantFFmpegURL: "rtmp://192.168.1.12:1935/live/app",
			wantDockerURL: "rtmp://192.168.1.12:1935/live/app",
			expectError:   false,
		},
		{
			name:          "srt listener",
			rawURL:        "srt://0.0.0.0:9000",
			wantListen:    true,
			wantPort:      "9000",
			wantFFmpegURL: "srt://0.0.0.0:9000?mode=listener",
			wantDockerURL: "srt://0.0.0.0:9000?mode=listener",
			expectError:   false,
		},
		{
			name:          "srt pull/caller",
			rawURL:        "srt://192.168.1.12:9000",
			wantListen:    false,
			wantPort:      "9000",
			wantFFmpegURL: "srt://192.168.1.12:9000?mode=caller",
			wantDockerURL: "srt://192.168.1.12:9000?mode=caller",
			expectError:   false,
		},
		{
			name:          "udp listener",
			rawURL:        "udp://0.0.0.0:1234",
			wantListen:    true,
			wantPort:      "1234",
			wantFFmpegURL: "udp://0.0.0.0:1234?listen",
			wantDockerURL: "udp://0.0.0.0:1234?listen",
			expectError:   false,
		},
		{
			name:          "udp multicast pull",
			rawURL:        "udp://239.1.1.1:1234",
			wantListen:    false,
			wantPort:      "1234",
			wantFFmpegURL: "udp://239.1.1.1:1234",
			wantDockerURL: "udp://239.1.1.1:1234",
			expectError:   false,
		},
		{
			name:          "http pull",
			rawURL:        "http://example.com/stream.m3u8",
			wantListen:    false,
			wantPort:      "",
			wantFFmpegURL: "http://example.com/stream.m3u8",
			wantDockerURL: "http://example.com/stream.m3u8",
			expectError:   false,
		},
		{
			name:          "localhost dockerization",
			rawURL:        "rtmp://localhost:1935/live/app",
			wantListen:    false,
			wantPort:      "1935",
			wantFFmpegURL: "rtmp://localhost:1935/live/app",
			wantDockerURL: "rtmp://host.docker.internal:1935/live/app",
			expectError:   false,
		},
		{
			name:          "127.0.0.1 dockerization",
			rawURL:        "rtmp://127.0.0.1:1935/live/app",
			wantListen:    false,
			wantPort:      "1935",
			wantFFmpegURL: "rtmp://127.0.0.1:1935/live/app",
			wantDockerURL: "rtmp://host.docker.internal:1935/live/app",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := ParseInputURL(tt.rawURL)
			if (err != nil) != tt.expectError {
				t.Fatalf("ParseInputURL() error = %v, expectError = %v", err, tt.expectError)
			}
			if tt.expectError {
				return
			}
			if spec.IsListener != tt.wantListen {
				t.Errorf("IsListener = %v, want %v", spec.IsListener, tt.wantListen)
			}
			if spec.ListenPort != tt.wantPort {
				t.Errorf("ListenPort = %q, want %q", spec.ListenPort, tt.wantPort)
			}
			if spec.FFmpegInputURL != tt.wantFFmpegURL {
				t.Errorf("FFmpegInputURL = %q, want %q", spec.FFmpegInputURL, tt.wantFFmpegURL)
			}
			if wantAlias := strings.Contains(tt.wantDockerURL, DockerHostAlias); spec.NeedsHostAlias != wantAlias {
				t.Errorf("NeedsHostAlias = %v, want %v", spec.NeedsHostAlias, wantAlias)
			}
			if spec.DockerInputURL != tt.wantDockerURL {
				t.Errorf("DockerInputURL = %q, want %q", spec.DockerInputURL, tt.wantDockerURL)
			}
		})
	}
}

func TestIsZeroHost(t *testing.T) {
	zeros := []string{"0", "0.0.0.0", "::", "0::0", "[::]", "[0::0]", "  0.0.0.0  "}
	for _, z := range zeros {
		if !isZeroHost(z) {
			t.Errorf("expected %q to be classified as zero host", z)
		}
	}

	nonZeros := []string{"127.0.0.1", "localhost", "192.168.1.1", "google.com"}
	for _, nz := range nonZeros {
		if isZeroHost(nz) {
			t.Errorf("expected %q to NOT be classified as zero host", nz)
		}
	}
}

func TestPublishArgUsesCorrectProtocol(t *testing.T) {
	tests := map[string]string{
		"rtmp://0.0.0.0:1935": "1935:1935/tcp",
		"srt://0.0.0.0:9000":  "9000:9000/udp",
		"udp://0.0.0.0:1234":  "1234:1234/udp",
	}
	for raw, want := range tests {
		spec, err := ParseInputURL(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := spec.PublishArg(); got != want {
			t.Errorf("PublishArg(%s) = %q, want %q", raw, got, want)
		}
	}
}
