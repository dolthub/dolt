package randbuf

import (
	"math/rand"
)

func RandBuf(r *rand.Rand, length int) []byte {
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = byte(r.Intn(256))
	}
	return buf[:]
}
