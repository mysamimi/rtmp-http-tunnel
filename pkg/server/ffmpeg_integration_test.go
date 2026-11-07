package server

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The server concatenates chunks from independent client sessions into one FFmpeg
// process. Each session restarts its MPEG-TS timestamps near zero, so this checks
// (with real FFmpeg) that the restreamer's pipeline still yields monotonic timestamps.
// It documents an assumption about FFmpeg rather than testing our own code.
func TestRestreamPipelineSurvivesTimestampReset(t *testing.T) {
	for _, bin := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not available", bin)
		}
	}
	// The fake ffmpeg used by other tests may shadow the real one only within those tests.
	dir := t.TempDir()
	run := func(name string, args ...string) {
		t.Helper()
		cmd := exec.Command(name, args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, out)
		}
	}

	run("ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc=size=320x240:rate=25",
		"-f", "lavfi", "-i", "sine=frequency=440", "-t", "10",
		"-c:v", "libx264", "-g", "50", "-keyint_min", "50", "-sc_threshold", "0", "-c:a", "aac", "-f", "flv", "src.flv")
	for _, r := range []string{"r1", "r2"} { // two client sessions: both start at the same timestamps
		run("ffmpeg", "-loglevel", "error", "-i", "src.flv", "-t", "8", "-c", "copy", "-f", "hls", "-hls_time", "2",
			"-hls_list_size", "9999", "-hls_segment_filename", r+"_chunk_%d.ts", r+".m3u8")
	}

	var stream bytes.Buffer
	for _, name := range []string{"r1_chunk_0.ts", "r1_chunk_1.ts", "r2_chunk_0.ts", "r2_chunk_1.ts", "r2_chunk_2.ts"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		stream.Write(data)
	}

	// Same arguments as Streamer, writing to a file instead of RTMP and without -re.
	out := filepath.Join(dir, "out.flv")
	cmd := exec.Command("ffmpeg", "-loglevel", "error", "-nostdin", "-f", "mpegts", "-i", "-", "-c", "copy", "-f", "flv", out)
	cmd.Stdin = &stream
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("restream ffmpeg failed: %v\n%s", err, b)
	}

	for _, sel := range []string{"v", "a"} {
		probe, err := exec.Command("ffprobe", "-v", "error", "-select_streams", sel,
			"-show_entries", "packet=dts_time", "-of", "csv=p=0", out).Output()
		if err != nil {
			t.Fatal(err)
		}
		prev, n := -1.0, 0
		for _, line := range strings.Fields(string(probe)) {
			v, err := strconv.ParseFloat(strings.TrimSuffix(line, ","), 64)
			if err != nil {
				continue
			}
			if v < prev {
				t.Fatalf("%s stream DTS went backwards at packet %d: %v after %v", sel, n, v, prev)
			}
			prev, n = v, n+1
		}
		if n == 0 {
			t.Fatalf("no %s packets in output", sel)
		}
		if sel == "v" && prev < 9 {
			t.Fatal(fmt.Sprintf("output too short (last video dts %.2fs); timestamps were not carried across sessions", prev))
		}
	}
}
