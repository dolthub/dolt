package secretkey

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"testing"
)

var tests = []struct {
	key          Key
	passphrase   []byte
	encryptedHex string
}{
	{
		Key{},
		nil,
		"78677021ad7fd0e9c230b0eb4f1c4144afa6af8e333dfbb64a3b18b4ccae081e2b32db6cff",
	},
	{
		Key{},
		[]byte("hello world"),
		"e3c5d22321e4d129e4e787b356321c7e9998fa0448466d258c069eaa7b6112ce2b32db6cff",
	},
	{
		Key{0xad, 0x43, 0x72, 0xbd, 0x96, 0xcb, 0xcd, 0x74, 0x0, 0x9, 0xa7, 0xc6, 0x61, 0xe6, 0xba, 0xbc, 0x9b, 0x5f, 0xde, 0x9d, 0x96, 0x18, 0xc0, 0xe9, 0x2d, 0x25, 0xd2, 0x2e, 0x17, 0xfc, 0x82, 0x92},
		[]byte("hello world"),
		"d84f3ee3787d7fc4b17e5976036b7ca398f7f6e1e6d1221ca0204cf18bed4c8245182306ff",
	},
	{
		Key{0xad, 0x43, 0x72, 0xbd, 0x96, 0xcb, 0xcd, 0x74, 0x0, 0x9, 0xa7, 0xc6, 0x61, 0xe6, 0xba, 0xbc, 0x9b, 0x5f, 0xde, 0x9d, 0x96, 0x18, 0xc0, 0xe9, 0x2d, 0x25, 0xd2, 0x2e, 0x17, 0xfc, 0x82, 0x92},
		[]byte("\x00\x01Keep this passphrase secret!"),
		"81d85d3c3e962e9825ed42b9803e62058c52fae7c557e5a72ceecec899e3b8bf45182306ff",
	},
	{
		Key{0x8b, 0xaf, 0x3c, 0x46, 0x51, 0x26, 0xb0, 0x5f, 0xe8, 0x8, 0xfa, 0x2c, 0xae, 0x60, 0x9, 0xe7, 0x3d, 0xc8, 0x7b, 0x8b, 0x8e, 0x13, 0x21, 0xce, 0x41, 0x20, 0x6f, 0x4e, 0x11, 0xf6, 0x1a, 0xb9},
		[]byte{0x8b, 0xaf, 0x3c, 0x46, 0x51, 0x26, 0xb0, 0x5f, 0xe8, 0x8, 0xfa, 0x2c, 0xae, 0x60, 0x9, 0xe7, 0x3d, 0xc8, 0x7b, 0x8b, 0x8e, 0x13, 0x21, 0xce, 0x41, 0x20, 0x6f, 0x4e, 0x11, 0xf6, 0x1a, 0xb9},
		"3cf48c3033b60f6a1d0bcf3b8d75debe5d8a27d4824db77b3a412fa8b915c2306fcfa236ff",
	},
}

func TestVectors(t *testing.T) {
	for i, test := range tests {
		x := Encrypt(&test.key, test.passphrase)
		encrypted, _ := hex.DecodeString(test.encryptedHex)
		if !bytes.Equal(x, encrypted) {
			t.Errorf("test %d\nexpected: %x\nactually: %x", i, encrypted, x)
		}

		key, ok := Decrypt(encrypted, test.passphrase)
		if !ok {
			t.Errorf("test %d: Decrypt failed.", i)
		}
		if !bytes.Equal(key[:], test.key[:]) {
			t.Errorf("test %d\nexpected: %x\nactually: %x", i, test.key, key)
		}
	}
}

func TestEncryptDecrypt(t *testing.T) {
	for i := 0; i < 10; i++ {
		key := New()
		passphrase := make([]byte, i)
		if _, err := rand.Read(passphrase); err != nil {
			t.Fatalf("rand.Read: %s", err)
		}

		c := Encrypt(key, passphrase)
		k, ok := Decrypt(c, passphrase)
		if !ok {
			t.Fatalf("hash check failed")
		}
		if !bytes.Equal(k[:], key[:]) {
			t.Fatalf("expected: %x\nactually: %x", key, k)
		}

		c[i]++
		_, ok = Decrypt(c, passphrase)
		if ok {
			t.Fatalf("expecting hash check to fail")
		}
	}
}

func TestEncode(t *testing.T) {
	for i := 0; i < 1000; i++ {
		x := make([]byte, i)
		if _, err := rand.Read(x); err != nil {
			t.Fatalf("rand.Read: %s", err)
		}
		enc := Encode(x)
		dec, err := Decode(enc)
		if err != nil {
			t.Fatalf("Decode: %s", err)
		}
		if !bytes.Equal(dec, x) {
			t.Fatalf("expected: %x\nactually: %x", x, dec)
		}
	}
}

func BenchmarkEncrypt(b *testing.B) {
	key := New()
	passphrase := []byte("secretkey passphrase")
	for i := 0; i < b.N; i++ {
		Encrypt(key, passphrase)
	}
}
