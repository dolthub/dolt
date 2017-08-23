package pnet

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
)

func newLine() io.Reader {
	return bytes.NewReader([]byte("\n"))
}

// GenerateV1PSK generates new PSK key that can be used with NewProtector
func GenerateV1PSK() io.Reader {
	psk := make([]byte, 32)
	rand.Read(psk)
	hexPsk := make([]byte, len(psk)*2)
	hex.Encode(hexPsk, psk)

	// just a shortcut to NewReader
	nr := func(b []byte) io.Reader {
		return bytes.NewReader(b)
	}
	return io.MultiReader(nr(pathPSKv1), newLine(), nr([]byte("/base16/")), newLine(), nr(hexPsk))
}
