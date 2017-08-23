package ringreader

import (
	"bytes"
	"fmt"
	"math/rand"

	random "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-random"
)

type Reader struct {
	Buf []byte
}

func NewReader(bufsize int) (*Reader, error) {
	buf := bytes.NewBuffer(nil)
	err := random.WritePseudoRandomBytes(int64(bufsize), buf, rand.Int63())
	return &Reader{Buf: buf.Bytes()}, err
}

func (r *Reader) Read(buf []byte) (n int, err error) {
	ibufl := len(r.Buf)
	left := len(buf)
	copied := 0

	for copied < left {
		pos1 := rand.Intn(len(r.Buf))
		pos2 := pos1 + left
		if pos2 > ibufl {
			pos2 = ibufl
		}
		copied += copy(buf[copied:], r.Buf[pos1:pos2])
	}

	if copied != left {
		err := fmt.Errorf("copied a different ammount: %d != %d", copied, left)
		panic(err.Error())
	}
	return copied, nil
}
