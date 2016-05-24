package blake2

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"testing"
)

func benchmarkHash(b *testing.B, hash func() hash.Hash) {
	b.SetBytes(1024 * 1024)
	data := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		h := hash()
		for j := 0; j < 1024; j++ {
			h.Write(data)
		}
		h.Sum(nil)
	}
}

func BenchmarkBlake2B(b *testing.B) {
	benchmarkHash(b, NewBlake2B)
}

func BenchmarkMD5(b *testing.B) {
	benchmarkHash(b, md5.New)
}

func BenchmarkSHA1(b *testing.B) {
	benchmarkHash(b, sha1.New)
}

func BenchmarkSHA256(b *testing.B) {
	benchmarkHash(b, sha256.New)
}

func BenchmarkSHA512(b *testing.B) {
	benchmarkHash(b, sha512.New)
}
