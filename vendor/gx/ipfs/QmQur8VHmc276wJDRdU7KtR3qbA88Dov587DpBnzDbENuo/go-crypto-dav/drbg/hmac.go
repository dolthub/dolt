// Based on HMAC_DRBG in NIST SP SP 800-90A
// http://csrc.nist.gov/publications/nistpubs/800-90A/SP800-90A.pdf
package drbg

import (
	"crypto/hmac"
	"crypto/sha512"
)

type DRBG struct {
	key []byte
	val []byte
}

func New(seed []byte) *DRBG {
	rng := DRBG{
		key: make([]byte, 64),
		val: make([]byte, 64),
	}

	for i := range rng.val {
		rng.val[i] = 1
	}

	rng.Reseed(seed)

	return &rng
}

func (rng *DRBG) Reseed(seed []byte) {
	rng.update(seed)
}

func (rng *DRBG) Read(b []byte) (n int, err error) {
	m := len(b)

	for n < m {
		rng.val = sum(rng.key, rng.val)
		n += copy(b[n:], rng.val)
	}

	rng.update(nil)

	return
}

func (rng *DRBG) update(data []byte) {
	rng.key = sum(rng.key, append(append(rng.val, 0), data...))
	rng.val = sum(rng.key, rng.val)

	if data != nil {
		rng.key = sum(rng.key, append(append(rng.val, 1), data...))
		rng.val = sum(rng.key, rng.val)
	}
}

func sum(key []byte, val []byte) []byte {
	h := hmac.New(sha512.New, key)
	h.Write(val)
	return h.Sum(nil)
}
