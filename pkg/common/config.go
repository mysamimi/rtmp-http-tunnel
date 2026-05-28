package common

import (
	"encoding/json"
	"os"
)

type StreamConfig struct {
	StreamPath    string `json:"stream_path"`
	LocalRTMPAddr string `json:"local_rtmp_addr"`
	TempDir       string `json:"temp_dir"`
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
