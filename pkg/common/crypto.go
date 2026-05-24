package common

// XORApply applies (or removes) XOR obfuscation to the data byte slice using the provided key.
// Since XOR is its own inverse, the same function is used for both encryption and decryption.
func XORApply(data []byte, key string) []byte {
	if key == "" {
		return data
	}
	keyBytes := []byte(key)
	keyLen := len(keyBytes)
	
	obfuscated := make([]byte, len(data))
	for i := 0; i < len(data); i++ {
		obfuscated[i] = data[i] ^ keyBytes[i%keyLen]
	}
	return obfuscated
}
