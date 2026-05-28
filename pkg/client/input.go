package client

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

type InputSpec struct {
	IsListener     bool   // Whether FFmpeg should listen
	FFmpegInputURL string // URL passed to -i
	ListenPort     string // Port for Docker port mapping (only in listen mode)
	DockerInputURL string // Docker-ized URL version (127.0.0.1/localhost -> 0.0.0.0)
}

func isZeroHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimPrefix(host, "[")
	host = strings.TrimSuffix(host, "]")
	return host == "0" || host == "0.0.0.0" || host == "::" || host == "0::0"
}

func ParseInputURL(rawURL string) (InputSpec, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return InputSpec{}, fmt.Errorf("failed to parse url: %w", err)
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme == "" {
		return InputSpec{}, fmt.Errorf("missing scheme in input URL: %s", rawURL)
	}

	// Extract hostname and port
	hostName, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		hostName = u.Host
		port = ""
	}

	isListen := isZeroHost(hostName)

	// For http/https, it's always pull (IsListener=false)
	if scheme == "http" || scheme == "https" {
		isListen = false
	}

	spec := InputSpec{
		IsListener: isListen,
		ListenPort: port,
	}

	// Construct FFmpegInputURL
	ffmpegURL := rawURL
	if isListen {
		switch scheme {
		case "srt":
			// srt + listener: add mode=listener
			if u.RawQuery == "" {
				ffmpegURL = rawURL + "?mode=listener"
			} else if !strings.Contains(u.RawQuery, "mode=") {
				ffmpegURL = rawURL + "&mode=listener"
			}
		case "udp":
			// udp + listener: add listen
			if u.RawQuery == "" {
				ffmpegURL = rawURL + "?listen"
			} else if !strings.Contains(u.RawQuery, "listen") {
				ffmpegURL = rawURL + "&listen"
			}
		}
	} else {
		// connect/pull modes
		switch scheme {
		case "srt":
			// srt + caller: add mode=caller
			if u.RawQuery == "" {
				ffmpegURL = rawURL + "?mode=caller"
			} else if !strings.Contains(u.RawQuery, "mode=") {
				ffmpegURL = rawURL + "&mode=caller"
			}
		}
	}
	spec.FFmpegInputURL = ffmpegURL

	// Construct DockerInputURL by replacing 127.0.0.1 and localhost with 0.0.0.0
	spec.DockerInputURL = dockerizeURL(ffmpegURL)

	return spec, nil
}

func dockerizeURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	hostName, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		hostName = u.Host
		port = ""
	}
	if hostName == "127.0.0.1" || hostName == "localhost" {
		if port != "" {
			u.Host = "0.0.0.0:" + port
		} else {
			u.Host = "0.0.0.0"
		}
		return u.String()
	}
	return rawURL
}
