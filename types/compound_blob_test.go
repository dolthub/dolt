package types

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func getTestCompoundBlob(datas ...string) compoundBlob {
	blobs := make([]Future, len(datas))
	childLengths := make([]uint64, len(datas))
	length := uint64(0)
	for i, s := range datas {
		b, _ := NewBlob(bytes.NewBufferString(s))
		blobs[i] = futureFromValue(b)
		childLengths[i] = uint64(len(s))
		length += uint64(len(s))
	}
	return compoundBlob{length, childLengths, blobs, &ref.Ref{}, nil}
}

func getAliceBlob(t *testing.T) compoundBlob {
	assert := assert.New(t)
	f, err := os.Open("alice-short.txt")
	assert.NoError(err)
	defer f.Close()

	b, err := NewBlob(f)
	assert.NoError(err)
	cb, ok := b.(compoundBlob)
	assert.True(ok)
	return cb
}

func TestCompoundBlobReader(t *testing.T) {
	assert := assert.New(t)
	cb := getTestCompoundBlob("hello", "world")
	bs, err := ioutil.ReadAll(cb.Reader())
	assert.NoError(err)
	assert.Equal("helloworld", string(bs))

	ab := getAliceBlob(t)
	bs, err = ioutil.ReadAll(ab.Reader())
	assert.NoError(err)
	f, err := os.Open("alice-short.txt")
	assert.NoError(err)
	defer f.Close()
	bs2, err := ioutil.ReadAll(f)
	assert.Equal(bs2, bs)
}

func TestCompoundBlobLen(t *testing.T) {
	assert := assert.New(t)
	cb := getTestCompoundBlob("hello", "world")
	assert.Equal(uint64(10), cb.Len())

	ab := getAliceBlob(t)
	assert.Equal(uint64(30157), ab.Len())
}

func TestCompoundBlobChunks(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.MemoryStore{}

	cb := getTestCompoundBlob("hello", "world")
	assert.Equal(0, len(cb.Chunks()))

	bl1 := newBlobLeaf([]byte("hello"))
	blr1 := bl1.Ref()
	bl2 := newBlobLeaf([]byte("world"))
	cb = compoundBlob{uint64(10), []uint64{5, 5}, []Future{futureFromRef(blr1), futureFromValue(bl2)}, &ref.Ref{}, cs}
	assert.Equal(1, len(cb.Chunks()))
}
