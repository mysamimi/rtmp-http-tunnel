package common

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
)

type StreamConfig struct {
	StreamPath string `json:"stream_path"`
	InputURL   string `json:"input_url"`
	TempDir    string `json:"temp_dir"`
}

type ClientConfig struct {
	ServerURL        string         `json:"server_url"`
	AuthToken        string         `json:"auth_token"`
	ObfuscationKey   string         `json:"obfuscation_key"`
	Concurrency      int            `json:"concurrency"`
	ChunkDuration    int            `json:"chunk_duration"`
	UseDocker        bool           `json:"use_docker"`
	DockerImage      string         `json:"docker_image"`
	MaxUploadRetries int            `json:"max_upload_retries"`
	Streams          []StreamConfig `json:"streams"`
}

type ServerStreamConfig struct {
	Path          string `json:"path"`
	TargetRTMPURL string `json:"target_rtmp_url"`
	TempDir       string `json:"temp_dir"`
}

type ServerConfig struct {
	ListenAddr             string               `json:"listen_addr"`
	AuthToken              string               `json:"auth_token"`
	ObfuscationKey         string               `json:"obfuscation_key"`
	BufferDuration         int                  `json:"buffer_duration"`
	RebufferDuration       int                  `json:"rebuffer_duration"`
	ChunkDuration          int                  `json:"chunk_duration"`
	InactiveSessionTimeout int                  `json:"inactive_session_timeout"` // seconds
	TLSCertFile            string               `json:"tls_cert_file"`
	TLSKeyFile             string               `json:"tls_key_file"`
	UseDocker              bool                 `json:"use_docker"`
	DockerImage            string               `json:"docker_image"`
	Streams                []ServerStreamConfig `json:"streams"`
}

type Config struct {
	Mode   string       `json:"mode"` // "client" or "server"
	Client ClientConfig `json:"client"`
	Server ServerConfig `json:"server"`
}

// LoadConfig reads and parses the JSON configuration file
func LoadConfig(filePath string) (*Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cfg Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (cfg *Config) Validate(mode string) error {
	switch mode {
	case "client":
		return validateClientConfig(cfg.Client)
	case "server":
		return validateServerConfig(cfg.Server)
	default:
		return fmt.Errorf("invalid mode %q: must be 'client' or 'server'", mode)
	}
}

func validateClientConfig(cfg ClientConfig) error {
	if err := requireNonEmpty("client.server_url", cfg.ServerURL); err != nil {
		return err
	}
	if err := validateHTTPURL("client.server_url", cfg.ServerURL); err != nil {
		return err
	}
	if err := requireNonEmpty("client.auth_token", cfg.AuthToken); err != nil {
		return err
	}
	if err := requireNonEmpty("client.obfuscation_key", cfg.ObfuscationKey); err != nil {
		return err
	}
	if cfg.ChunkDuration <= 0 {
		return fmt.Errorf("client.chunk_duration must be greater than zero")
	}
	if len(cfg.Streams) == 0 {
		return fmt.Errorf("client.streams must contain at least one stream")
	}

	seen := make(map[string]struct{}, len(cfg.Streams))
	for i, stream := range cfg.Streams {
		prefix := fmt.Sprintf("client.streams[%d]", i)
		if err := validateStreamPath(prefix+".stream_path", stream.StreamPath); err != nil {
			return err
		}
		if _, ok := seen[stream.StreamPath]; ok {
			return fmt.Errorf("%s.stream_path %q is duplicated", prefix, stream.StreamPath)
		}
		seen[stream.StreamPath] = struct{}{}
		if err := requireNonEmpty(prefix+".input_url", stream.InputURL); err != nil {
			return err
		}
		if _, err := url.ParseRequestURI(stream.InputURL); err != nil {
			return fmt.Errorf("%s.input_url is invalid: %w", prefix, err)
		}
		if err := requireNonEmpty(prefix+".temp_dir", stream.TempDir); err != nil {
			return err
		}
	}

	return nil
}

func validateServerConfig(cfg ServerConfig) error {
	if err := requireNonEmpty("server.listen_addr", cfg.ListenAddr); err != nil {
		return err
	}
	if err := requireNonEmpty("server.auth_token", cfg.AuthToken); err != nil {
		return err
	}
	if err := requireNonEmpty("server.obfuscation_key", cfg.ObfuscationKey); err != nil {
		return err
	}
	if cfg.ChunkDuration <= 0 {
		return fmt.Errorf("server.chunk_duration must be greater than zero")
	}
	if cfg.BufferDuration <= cfg.ChunkDuration {
		return fmt.Errorf("server.buffer_duration must be greater than server.chunk_duration")
	}
	if cfg.RebufferDuration < 0 {
		return fmt.Errorf("server.rebuffer_duration must not be negative")
	}
	if cfg.InactiveSessionTimeout <= 0 {
		return fmt.Errorf("server.inactive_session_timeout must be greater than zero")
	}
	if (cfg.TLSCertFile == "") != (cfg.TLSKeyFile == "") {
		return fmt.Errorf("server.tls_cert_file and server.tls_key_file must be configured together")
	}
	if len(cfg.Streams) == 0 {
		return fmt.Errorf("server.streams must contain at least one stream")
	}

	seen := make(map[string]struct{}, len(cfg.Streams))
	for i, stream := range cfg.Streams {
		prefix := fmt.Sprintf("server.streams[%d]", i)
		if err := validateStreamPath(prefix+".path", stream.Path); err != nil {
			return err
		}
		if _, ok := seen[stream.Path]; ok {
			return fmt.Errorf("%s.path %q is duplicated", prefix, stream.Path)
		}
		seen[stream.Path] = struct{}{}
		if err := requireNonEmpty(prefix+".target_rtmp_url", stream.TargetRTMPURL); err != nil {
			return err
		}
		if err := requireNonEmpty(prefix+".temp_dir", stream.TempDir); err != nil {
			return err
		}
	}

	return nil
}

func requireNonEmpty(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	return nil
}

func validateHTTPURL(name, rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%s is invalid: %w", name, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%s must use http or https", name)
	}
	if parsed.Host == "" {
		return fmt.Errorf("%s must include a host", name)
	}
	return nil
}

func validateStreamPath(name, value string) error {
	if err := requireNonEmpty(name, value); err != nil {
		return err
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return fmt.Errorf("%s %q contains invalid character %q; use letters, numbers, '.', '_' or '-'", name, value, r)
		}
	}
	return nil
}
