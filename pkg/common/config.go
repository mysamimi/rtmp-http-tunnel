package common

import (
	"encoding/json"
	"os"
)

type ClientConfig struct {
	LocalRTMPAddr  string `json:"local_rtmp_addr"`  // e.g., "127.0.0.1:1935"
	ServerURL      string `json:"server_url"`       // e.g., "https://server:8443"
	AuthToken      string `json:"auth_token"`       // Shared secret for header authentication
	Concurrency    int    `json:"concurrency"`      // Number of concurrent uploaders
	ChunkDuration  int    `json:"chunk_duration"`   // Target duration for each TS chunk in seconds
	ObfuscationKey string `json:"obfuscation_key"`  // Key for XOR encrypting chunk data
	TempDir        string `json:"temp_dir"`         // Temp dir to store generated TS files
}

type ServerConfig struct {
	ListenAddr     string `json:"listen_addr"`      // e.g., ":8443"
	AuthToken      string `json:"auth_token"`       // Must match client's AuthToken
	ObfuscationKey string `json:"obfuscation_key"`  // Key for XOR decrypting chunk data
	BufferDuration int    `json:"buffer_duration"`  // Buffer size in seconds (e.g., 60)
	ChunkDuration  int    `json:"chunk_duration"`   // Expected duration of each chunk (e.g., 2)
	TargetRTMPURL  string `json:"target_rtmp_url"`  // YouTube/Aparat RTMP URL + Stream Key
	TempDir        string `json:"temp_dir"`         // Temp dir to store received TS files
	TLSCertFile    string `json:"tls_cert_file"`    // Optional: path to SSL cert file for HTTPS
	TLSKeyFile     string `json:"tls_key_file"`     // Optional: path to SSL key file for HTTPS
}

type Config struct {
	Mode   string       `json:"mode"`   // "client" or "server"
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
