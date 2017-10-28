package pnet

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func bufWithBase(base string) *bytes.Buffer {

	b := &bytes.Buffer{}
	b.Write(pathPSKv1)
	b.WriteString("\n")
	b.WriteString(base)
	b.WriteString("\n")
	return b
}

func TestDecodeHex(t *testing.T) {
	b := bufWithBase("/base16/")
	for i := 0; i < 32; i++ {
		b.WriteString("FF")
	}

	psk, err := decodeV1PSK(b)
	if err != nil {
		t.Fatal(err)
	}

	for _, b := range psk {
		if b != 255 {
			t.Fatal("byte was wrong")
		}
	}
}

func TestDecodeB64(t *testing.T) {
	b := bufWithBase("/base64/")
	key := make([]byte, 32)
	for i := 0; i < 32; i++ {
		key[i] = byte(i)
	}

	e := base64.NewEncoder(base64.StdEncoding, b)
	_, err := e.Write(key)
	if err != nil {
		t.Fatal(err)
	}

	psk, err := decodeV1PSK(b)
	if err != nil {
		t.Fatal(err)
	}

	for i, b := range psk {
		if b != psk[i] {
			t.Fatal("byte was wrong")
		}
	}

}
