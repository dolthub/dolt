package keyspace

import (
	"bytes"
	"crypto/sha256"
	"math/big"
)

// XORKeySpace is a KeySpace which:
// - normalizes identifiers using a cryptographic hash (sha256)
// - measures distance by XORing keys together
var XORKeySpace = &xorKeySpace{}
var _ KeySpace = XORKeySpace // ensure it conforms

type xorKeySpace struct{}

// Key converts an identifier into a Key in this space.
func (s *xorKeySpace) Key(id []byte) Key {
	hash := sha256.Sum256(id)
	key := hash[:]
	return Key{
		Space:    s,
		Original: id,
		Bytes:    key,
	}
}

// Equal returns whether keys are equal in this key space
func (s *xorKeySpace) Equal(k1, k2 Key) bool {
	return bytes.Equal(k1.Bytes, k2.Bytes)
}

// Distance returns the distance metric in this key space
func (s *xorKeySpace) Distance(k1, k2 Key) *big.Int {
	// XOR the keys
	k3 := XOR(k1.Bytes, k2.Bytes)

	// interpret it as an integer
	dist := big.NewInt(0).SetBytes(k3)
	return dist
}

// Less returns whether the first key is smaller than the second.
func (s *xorKeySpace) Less(k1, k2 Key) bool {
	a := k1.Bytes
	b := k2.Bytes
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return true
}

// ZeroPrefixLen returns the number of consecutive zeroes in a byte slice.
func ZeroPrefixLen(id []byte) int {
	for i := 0; i < len(id); i++ {
		for j := 0; j < 8; j++ {
			if (id[i]>>uint8(7-j))&0x1 != 0 {
				return i*8 + j
			}
		}
	}
	return len(id) * 8
}

// XOR takes two byte slices, XORs them together, returns the resulting slice.
func XOR(a, b []byte) []byte {
	c := make([]byte, len(a))
	for i := 0; i < len(a); i++ {
		c[i] = a[i] ^ b[i]
	}
	return c
}
