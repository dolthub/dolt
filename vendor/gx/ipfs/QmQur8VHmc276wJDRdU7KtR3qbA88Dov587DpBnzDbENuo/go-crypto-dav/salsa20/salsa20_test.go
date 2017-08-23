package salsa20

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"math/big"
	"testing"

	"gx/ipfs/QmaPHkZLbQQbvcyavn8q1GFHg6o6yeceyHFSJ3Pjf3p3TQ/go-crypto/salsa20"
)

func TestRandom(t *testing.T) {
	var key [32]byte
	nonce := make([]byte, 8)
	for i := 0; i < 10000; i++ {
		msg := make([]byte, i)
		rand.Read(key[:])
		rand.Read(nonce)
		rand.Read(msg)

		c0 := make([]byte, len(msg))
		c1 := make([]byte, len(msg))
		c2 := make([]byte, len(msg))
		salsa20.XORKeyStream(c0, msg, nonce, &key)
		XORKeyStream(c1, msg, nonce, &key)
		XORKeyStreamWriter(c2, msg, nonce, &key)

		if !bytes.Equal(c0, c1) {
			t.Fatalf("key=%x nonce=%x msg=%x\n  expected=%x\n  actually=%x", key, nonce, msg, c0, c1)
		}
		if !bytes.Equal(c1, c2) {
			t.Fatalf("key=%x nonce=%x msg=%x\n  expected=%x\n  actually=%x", key, nonce, msg, c1, c2)
		}

		// test truncated dst
		x := randInt(len(msg))
		c3 := make([]byte, len(msg)-x)
		c4 := make([]byte, len(msg)-x)
		salsa20.XORKeyStream(c3, msg, nonce, &key)
		XORKeyStream(c4, msg, nonce, &key)
		if !bytes.Equal(c3, c4) {
			t.Fatalf("key=%x nonce=%x msg=%x\n  expected=%x\n  actually=%x", key, nonce, msg, c3, c4)
		}
	}
}

func XORKeyStream(out, in []byte, nonce []byte, key *[32]byte) {
	c := New(key, nonce)
	c.XORKeyStream(out, in)
}

func XORKeyStreamWriter(out, in []byte, nonce []byte, key *[32]byte) {
	b := new(bytes.Buffer)
	w := &cipher.StreamWriter{S: New(key, nonce), W: b}
	for len(in) > 0 {
		i := randInt(len(in))
		if _, err := w.Write(in[:i]); err != nil {
			panic(err)
		}
		in = in[i:]
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	copy(out, b.Bytes())
}

func randInt(max int) int {
	m := big.NewInt(int64(max) + 1)
	n, err := rand.Int(rand.Reader, m)
	if err != nil {
		panic(err)
	}
	return int(n.Int64())
}
