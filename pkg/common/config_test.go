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

func TestConfigValidateRejectsNegativeRebufferDuration(t *testing.T) {
	cfg := validConfig()
	cfg.Server.RebufferDuration = -1

	if err := cfg.Validate("server"); err == nil {
		t.Fatal("Validate accepted negative rebuffer_duration")
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

func TestConfigValidateClientTLSPin(t *testing.T) {
	cfg := validConfig()
	cfg.Client.TLSPinSHA256 = "AB:CD"
	if err := cfg.Validate("client"); err == nil {
		t.Fatal("Validate accepted malformed tls_pin_sha256")
	}

	cfg.Client.TLSPinSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if err := cfg.Validate("client"); err != nil {
		t.Fatalf("Validate rejected valid pin: %v", err)
	}
	cfg.Client.TLSPinSHA256 = "E3:B0:C4:42:98:FC:1C:14:9A:FB:F4:C8:99:6F:B9:24:27:AE:41:E4:64:9B:93:4C:A4:95:99:1B:78:52:B8:55"
	if err := cfg.Validate("client"); err != nil {
		t.Fatalf("Validate rejected openssl-style pin: %v", err)
	}
}
