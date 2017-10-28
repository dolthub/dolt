package pnet

import (
	"gx/ipfs/QmaPHkZLbQQbvcyavn8q1GFHg6o6yeceyHFSJ3Pjf3p3TQ/go-crypto/salsa20"
	"gx/ipfs/QmaPHkZLbQQbvcyavn8q1GFHg6o6yeceyHFSJ3Pjf3p3TQ/go-crypto/sha3"
)

var zero64 = make([]byte, 64)

func fingerprint(psk *[32]byte) []byte {
	enc := make([]byte, 64)

	// We encrypt data first so we don't feed PSK to hash function.
	// Salsa20 function is not reversible thus increasing our security margin.
	salsa20.XORKeyStream(enc, zero64, []byte("finprint"), psk)

	out := make([]byte, 16)
	// Then do Shake-128 hash to reduce its length.
	// This way if for some reason Shake is broken and Salsa20 preimage is possible,
	// attacker has only half of the bytes necessary to recreate psk.
	sha3.ShakeSum128(out, enc)

	return out
}
