package common

import "testing"

func TestConfigValidateClient(t *testing.T) {
	cfg := validConfig()

	if err := cfg.Validate("client"); err != nil {
		t.Fatalf("Validate(client) returned error: %v", err)
	}
}

func TestConfigValidateServer(t *testing.T) {
	cfg := validConfig()

	if err := cfg.Validate("server"); err != nil {
		t.Fatalf("Validate(server) returned error: %v", err)
	}
}

func TestConfigValidateRejectsInvalidMode(t *testing.T) {
	cfg := validConfig()

	if err := cfg.Validate("worker"); err == nil {
		t.Fatal("Validate accepted invalid mode")
	}
}

func TestConfigValidateRejectsBadStreamPath(t *testing.T) {
	cfg := validConfig()
	cfg.Client.Streams[0].StreamPath = "foo/bar"

	if err := cfg.Validate("client"); err == nil {
		t.Fatal("Validate accepted stream path containing slash")
	}
}

func TestConfigValidateRejectsDuplicateServerPath(t *testing.T) {
	cfg := validConfig()
	cfg.Server.Streams = append(cfg.Server.Streams, cfg.Server.Streams[0])

	if err := cfg.Validate("server"); err == nil {
		t.Fatal("Validate accepted duplicate server stream path")
	}
}

func TestConfigValidateRejectsInvalidBuffer(t *testing.T) {
	cfg := validConfig()
	cfg.Server.BufferDuration = cfg.Server.ChunkDuration

	if err := cfg.Validate("server"); err == nil {
		t.Fatal("Validate accepted buffer_duration <= chunk_duration")
	}
}

func validConfig() *Config {
	return &Config{
		Mode: "client",
		Client: ClientConfig{
			ServerURL:      "https://example.com:8443",
			AuthToken:      "secret-token",
			ObfuscationKey: "xor-key",
			ChunkDuration:  2,
			Streams: []StreamConfig{
				{
					StreamPath: "stream_1",
					InputURL:   "rtmp://0.0.0.0:1935/live/app",
					TempDir:    "./tmp_client/stream_1",
				},
			},
		},
		Server: ServerConfig{
			ListenAddr:             ":8443",
			AuthToken:              "secret-token",
			ObfuscationKey:         "xor-key",
			BufferDuration:         60,
			ChunkDuration:          2,
			InactiveSessionTimeout: 120,
			Streams: []ServerStreamConfig{
				{
					Path:          "stream_1",
					TargetRTMPURL: "rtmp://a.rtmp.youtube.com/live2/key",
					TempDir:       "./tmp_server/stream_1",
				},
			},
		},
	}
}
