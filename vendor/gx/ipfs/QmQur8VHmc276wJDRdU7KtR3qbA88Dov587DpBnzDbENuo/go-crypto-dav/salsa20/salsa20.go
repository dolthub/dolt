package salsa20

import (
	"crypto/cipher"
	"encoding/binary"

	"gx/ipfs/QmaPHkZLbQQbvcyavn8q1GFHg6o6yeceyHFSJ3Pjf3p3TQ/go-crypto/salsa20/salsa"
)

const BlockSize = 64

type salsaCipher struct {
	key     *[32]byte
	nonce   [8]byte
	x       [BlockSize]byte
	nx      int
	counter uint64
}

func New(key *[32]byte, nonce []byte) cipher.Stream {
	c := new(salsaCipher)

	if len(nonce) == 24 {
		var subKey [32]byte
		var hNonce [16]byte
		copy(hNonce[:], nonce[:16])
		salsa.HSalsa20(&subKey, &hNonce, key, &salsa.Sigma)
		copy(c.nonce[:], nonce[16:])
		c.key = &subKey
	} else if len(nonce) == 8 {
		c.key = key
		copy(c.nonce[:], nonce)
	} else {
		panic("salsa20: nonce must be 8 or 24 bytes")
	}
	return c
}

func (c *salsaCipher) XORKeyStream(dst, src []byte) {
	if len(dst) < len(src) {
		src = src[:len(dst)]
	}
	if c.nx > 0 {
		n := xorBytes(dst, src, c.x[c.nx:])
		c.nx += n
		if c.nx == BlockSize {
			c.nx = 0
		}
		src = src[n:]
		dst = dst[n:]
	}
	if len(src) > BlockSize {
		n := len(src) &^ (BlockSize - 1)
		c.blocks(dst, src[:n])
		src = src[n:]
		dst = dst[n:]
	}
	if len(src) > 0 {
		c.nx = copy(c.x[:], src)
		for i := c.nx; i < len(c.x); i++ {
			c.x[i] = 0
		}
		c.blocks(c.x[:], c.x[:])
		copy(dst, c.x[:c.nx])
	}
}

func (c *salsaCipher) blocks(dst, src []byte) {
	var nonce [16]byte
	copy(nonce[:], c.nonce[:])
	binary.LittleEndian.PutUint64(nonce[8:], c.counter)
	salsa.XORKeyStream(dst, src, &nonce, c.key)
	c.counter += uint64(len(src)) / 64
}

func xorBytes(dst, a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		dst[i] = a[i] ^ b[i]
	}
	return n
}
