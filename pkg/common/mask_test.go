package common

import (
	"bytes"
	"strings"
	"testing"
)

func TestMaskURL(t *testing.T) {
	tests := map[string]string{
		"rtmp://a.rtmp.youtube.com/live2/abcd-efgh-ijkl": "rtmp://a.rtmp.youtube.com/live2/ab***",
		"rtmp://host/app/key":                            "rtmp://host/app/***",
		"rtmp://host/live/secretkey?token=xyz":           "rtmp://host/live/se***?***",
		"rtmp://user:pw@host/live/secretkey":             "rtmp://***@host/live/se***",
		"not a url":                                      "***",
	}
	for in, want := range tests {
		if got := MaskURL(in); got != want {
			t.Errorf("MaskURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMaskingWriterHidesKeyEvenWhenSplitAcrossWrites(t *testing.T) {
	const target = "rtmp://a.rtmp.youtube.com/live2/abcd-efgh-ijkl"
	var out bytes.Buffer
	w := NewMaskingWriter(&out, target)

	for _, part := range []string{"Output #0, flv, to '" + target[:35], target[35:] + "':\nretry with abcd-ef", "gh-ijkl failed\nlast line without newline abcd-efgh-ijkl"} {
		if _, err := w.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Flush()

	got := out.String()
	if strings.Contains(got, "efgh-ijkl") {
		t.Fatalf("stream key leaked: %q", got)
	}
	if !strings.Contains(got, "rtmp://a.rtmp.youtube.com/live2/ab***") || !strings.Contains(got, "retry with ab*** failed") {
		t.Fatalf("unexpected masked output: %q", got)
	}
}
