// Package e2e runs the real client and server against real FFmpeg processes: a synthetic
// encoder publishes MPEG-TS over UDP to the client, and a local TCP sink that receives FLV
// stands in for YouTube (so no RTMP server or listen-capable FFmpeg build is needed).
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mysamimi/rtmp-http-tunnel/pkg/client"
	"github.com/mysamimi/rtmp-http-tunnel/pkg/common"
	"github.com/mysamimi/rtmp-http-tunnel/pkg/server"
)

func requireTools(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("end-to-end test skipped in -short mode")
	}
	for _, bin := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not available", bin)
		}
	}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// sink is a TCP server that stores everything FFmpeg sends it (FLV), one file per
// connection. Stopping it drops the connection like a target going away would.
type sink struct {
	ln   net.Listener
	mu   sync.Mutex
	conn net.Conn
	n    int
	dir  string
	name string
	done sync.WaitGroup
}

func startSink(t *testing.T, port int, name string) *sink {
	t.Helper()
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	s := &sink{ln: ln, dir: t.TempDir(), name: name}
	s.done.Add(1)
	go func() {
		defer s.done.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conn = conn
			path := s.fileLocked(s.n)
			s.n++
			s.mu.Unlock()
			f, err := os.Create(path)
			if err != nil {
				return
			}
			_, _ = io.Copy(f, conn)
			f.Close()
		}
	}()
	t.Cleanup(s.stop)
	return s
}

func (s *sink) fileLocked(i int) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s.%d.flv", s.name, i))
}

// file returns the path of the first received connection's data.
func (s *sink) file() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fileLocked(0)
}

func (s *sink) stop() {
	_ = s.ln.Close()
	s.mu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.mu.Unlock()
	s.done.Wait()
}

// videoDTS returns the decode timestamps of the video packets in an FLV file.
func videoDTS(t *testing.T, path string) []float64 {
	t.Helper()
	b, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "v",
		"-show_entries", "packet=dts_time", "-of", "csv=p=0", path).Output()
	if err != nil {
		return nil
	}
	var out []float64
	for _, f := range strings.Fields(string(b)) {
		if v, err := strconv.ParseFloat(strings.TrimSuffix(f, ","), 64); err == nil {
			out = append(out, v)
		}
	}
	return out
}

func span(dts []float64) float64 {
	if len(dts) == 0 {
		return 0
	}
	return dts[len(dts)-1] - dts[0]
}

func assertMonotonic(t *testing.T, dts []float64) {
	t.Helper()
	for i := 1; i < len(dts); i++ {
		if dts[i] < dts[i-1] {
			t.Fatalf("DTS went backwards at packet %d: %.3f after %.3f", i, dts[i], dts[i-1])
		}
	}
}

type pipeline struct {
	ingestPort int // UDP port the client listens on
	ytPort     int // TCP port of the stand-in for YouTube
}

// startTunnel launches the server and client in-process and returns the ports in use.
func startTunnel(t *testing.T) pipeline {
	t.Helper()
	p := pipeline{ingestPort: freeUDPPort(t), ytPort: freePort(t)}
	serverPort := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Log("tunnel did not shut down within 15s")
		}
	})

	srvCfg := &common.ServerConfig{
		ListenAddr:             fmt.Sprintf("127.0.0.1:%d", serverPort),
		AuthToken:              "tok",
		ObfuscationKey:         "key",
		BufferDuration:         4,
		ChunkDuration:          2,
		InactiveSessionTimeout: 60,
		Streams: []common.ServerStreamConfig{{
			Path:          "e2e",
			TargetRTMPURL: fmt.Sprintf("tcp://127.0.0.1:%d", p.ytPort),
			TempDir:       filepath.Join(t.TempDir(), "srv"),
		}},
	}
	cliCfg := &common.ClientConfig{
		ServerURL:      fmt.Sprintf("http://127.0.0.1:%d", serverPort),
		AuthToken:      "tok",
		ObfuscationKey: "key",
		Concurrency:    2,
		ChunkDuration:  2,
		Streams: []common.StreamConfig{{
			StreamPath: "e2e",
			InputURL:   fmt.Sprintf("udp://0.0.0.0:%d", p.ingestPort),
			TempDir:    filepath.Join(t.TempDir(), "cli"),
		}},
	}
	for name, cfg := range map[string]*common.Config{
		"server": {Server: *srvCfg},
		"client": {Client: *cliCfg},
	} {
		if err := cfg.Validate(name); err != nil {
			t.Fatalf("invalid %s config: %v", name, err)
		}
	}

	wg.Add(2)
	go func() { defer wg.Done(); _ = server.NewServer(srvCfg).Start(ctx) }()
	go func() { defer wg.Done(); _ = client.NewClient(cliCfg).Start(ctx) }()
	return p
}

// publish pushes a synthetic 2s-keyframe H.264/AAC stream in real time and returns when
// the encoder has finished. UDP is connectionless, so the first moments may be lost if the
// client's FFmpeg is not listening yet; give it a head start.
func publish(t *testing.T, port int, seconds int) {
	t.Helper()
	time.Sleep(3 * time.Second)
	var stderr bytes.Buffer
	cmd := exec.Command("ffmpeg", "-loglevel", "error", "-nostdin", "-re",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=25",
		"-f", "lavfi", "-i", "sine=frequency=440",
		"-t", strconv.Itoa(seconds),
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "50", "-keyint_min", "50", "-sc_threshold", "0",
		"-c:a", "aac", "-f", "mpegts", fmt.Sprintf("udp://127.0.0.1:%d?pkt_size=1316", port))
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Errorf("publisher failed: %v\n%s", err, stderr.String())
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestEndToEndStreamReachesTarget(t *testing.T) {
	requireTools(t)
	p := startTunnel(t)
	target := startSink(t, p.ytPort, "yt")

	publish(t, p.ingestPort, 16)

	waitFor(t, 40*time.Second, "restreamed output", func() bool { return span(videoDTS(t, target.file())) >= 8 })
	dts := videoDTS(t, target.file())
	assertMonotonic(t, dts)
	t.Logf("target received %.1fs of video (%d packets) from a 16s source", span(dts), len(dts))
}

func TestEndToEndSurvivesTargetDisconnect(t *testing.T) {
	requireTools(t)
	p := startTunnel(t)
	first := startSink(t, p.ytPort, "a")

	done := make(chan struct{})
	go func() { defer close(done); publish(t, p.ingestPort, 30) }()

	waitFor(t, 40*time.Second, "first target to receive video", func() bool { return span(videoDTS(t, first.file())) >= 2 })

	// The target drops the connection, stays away for a while, then comes back.
	first.stop()
	time.Sleep(3 * time.Second)
	second := startSink(t, p.ytPort, "b")

	// Streaming must resume from the preserved buffer, not after a full re-buffer.
	waitFor(t, 30*time.Second, "output to resume on the new target", func() bool { return span(videoDTS(t, second.file())) >= 4 })
	<-done
	dts := videoDTS(t, second.file())
	assertMonotonic(t, dts)
	t.Logf("after reconnect the target received %.1fs of video", span(dts))
}
