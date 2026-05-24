package common

import (
	"bytes"
	"testing"
)

func TestXORApply(t *testing.T) {
	key := "secret-key"
	original := []byte("Hello, World! This is a test stream chunk payload.")

	// Obfuscate
	obfuscated := XORApply(original, key)
	if bytes.Equal(original, obfuscated) {
		t.Error("Obfuscated data should not match original data")
	}

	// De-obfuscate (applying XOR twice should return original data)
	deobfuscated := XORApply(obfuscated, key)
	if !bytes.Equal(original, deobfuscated) {
		t.Error("Double XOR did not yield original data")
	}

	// Empty key test (should return identical data)
	noKey := XORApply(original, "")
	if !bytes.Equal(original, noKey) {
		t.Error("XOR with empty key should return unchanged data")
	}
}
