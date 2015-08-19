package types

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func getTestCompoundBlob(datas ...string) compoundBlob {
	blobs := make([]Future, len(datas))
	offsets := make([]uint64, len(datas))
	length := uint64(0)
	for i, s := range datas {
		b, _ := NewBlob(bytes.NewBufferString(s))
		blobs[i] = futureFromValue(b)
		length += uint64(len(s))
		offsets[i] = length
	}
	return compoundBlob{offsets, blobs, &ref.Ref{}, nil}
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
	cs := &chunks.MemoryStore{}

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

	ref := WriteValue(cb, cs)

	cb2 := ReadValue(ref, cs)
	bs3, err := ioutil.ReadAll(cb2.(Blob).Reader())
	assert.NoError(err)
	assert.Equal("helloworld", string(bs3))
}

type testBlob struct {
	blobLeaf
	readCount *int
}

func (b testBlob) Reader() io.ReadSeeker {
	*b.readCount++
	return b.blobLeaf.Reader()
}

func TestCompoundBlobReaderLazy(t *testing.T) {
	assert := assert.New(t)

	readCount1 := 0
	b1 := newBlobLeaf([]byte("hi"))
	tb1 := &testBlob{b1, &readCount1}

	readCount2 := 0
	b2 := newBlobLeaf([]byte("bye"))
	tb2 := &testBlob{b2, &readCount2}

	cb := compoundBlob{[]uint64{2, 5}, []Future{futureFromValue(tb1), futureFromValue(tb2)}, &ref.Ref{}, nil}

	r := cb.Reader()
	assert.Equal(0, readCount1)
	assert.Equal(0, readCount2)

	p := []byte{0}
	n, err := r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n)
	assert.Equal(1, readCount1)
	assert.Equal(0, readCount2)

	n, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n)
	assert.Equal(1, readCount1)
	assert.Equal(0, readCount2)

	n, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n)
	assert.Equal(1, readCount1)
	assert.Equal(1, readCount2)

	n, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n)
	assert.Equal(1, readCount1)
	assert.Equal(1, readCount2)
}

func TestCompoundBlobReaderLazySeek(t *testing.T) {
	assert := assert.New(t)

	readCount1 := 0
	b1 := newBlobLeaf([]byte("hi"))
	tb1 := &testBlob{b1, &readCount1}

	readCount2 := 0
	b2 := newBlobLeaf([]byte("bye"))
	tb2 := &testBlob{b2, &readCount2}

	cb := compoundBlob{[]uint64{2, 5}, []Future{futureFromValue(tb1), futureFromValue(tb2)}, &ref.Ref{}, nil}

	r := cb.Reader()

	_, err := r.Seek(0, 4)
	assert.Error(err)

	_, err = r.Seek(-1, 0)
	assert.Error(err)

	p := []byte{0}

	n, err := r.Seek(3, 0)
	assert.NoError(err)
	assert.Equal(int64(3), n)
	assert.Equal(0, readCount1)
	assert.Equal(1, readCount2)

	n2, err := r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n2)
	assert.Equal(0, readCount1)
	assert.Equal(1, readCount2)
	assert.Equal("y", string(p))

	n, err = r.Seek(-1, 1)
	assert.NoError(err)
	assert.Equal(int64(3), n)
	assert.Equal(0, readCount1)
	assert.Equal(1, readCount2)

	n2, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n2)
	assert.Equal(0, readCount1)
	assert.Equal(1, readCount2)
	assert.Equal("y", string(p))

	n, err = r.Seek(-5, 2)
	assert.NoError(err)
	assert.Equal(int64(0), n)
	assert.Equal(1, readCount1)
	assert.Equal(1, readCount2)

	n2, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n2)
	assert.Equal(1, readCount1)
	assert.Equal(1, readCount2)
	assert.Equal("h", string(p))

	n, err = r.Seek(100, 0)
	assert.NoError(err)
	assert.Equal(int64(100), n)
	assert.Equal(1, readCount1)
	assert.Equal(1, readCount2)

	n2, err = r.Read(p)
	assert.Equal(io.EOF, err)
	assert.Equal(0, n2)
	assert.Equal(1, readCount1)
	assert.Equal(1, readCount2)

	n, err = r.Seek(-99, 1)
	assert.NoError(err)
	assert.Equal(int64(1), n)
	assert.Equal(2, readCount1)
	assert.Equal(1, readCount2)

	n2, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n2)
	assert.Equal(2, readCount1)
	assert.Equal(1, readCount2)
	assert.Equal("i", string(p))

	n2, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n2)
	assert.Equal(2, readCount1)
	assert.Equal(2, readCount2)
	assert.Equal("b", string(p))
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
	cb = compoundBlob{[]uint64{5, 10}, []Future{futureFromRef(blr1), futureFromValue(bl2)}, &ref.Ref{}, cs}
	assert.Equal(1, len(cb.Chunks()))
}

func TestCompoundBlobSameChunksWithPrefix(t *testing.T) {
	assert := assert.New(t)

	cb1 := getAliceBlob(t)

	// Load same file again but prepend some data... all but the first chunk should stay the same
	f, err := os.Open("alice-short.txt")
	assert.NoError(err)
	defer f.Close()
	buf := bytes.NewBufferString("prefix")
	r := io.MultiReader(buf, f)

	b, err := NewBlob(r)
	assert.NoError(err)
	cb2 := b.(compoundBlob)

	assert.Equal(cb2.Len(), cb1.Len()+uint64(6))
	assert.Equal(3, len(cb1.blobs))
	assert.Equal(len(cb1.blobs), len(cb2.blobs))
	assert.NotEqual(cb1.blobs[0].Ref(), cb2.blobs[0].Ref())
	assert.Equal(cb1.blobs[1].Ref(), cb2.blobs[1].Ref())
	assert.Equal(cb1.blobs[2].Ref(), cb2.blobs[2].Ref())
}

func TestCompoundBlobSameChunksWithSuffix(t *testing.T) {
	assert := assert.New(t)

	cb1 := getAliceBlob(t)

	// Load same file again but append some data... all but the last chunk should stay the same
	f, err := os.Open("alice-short.txt")
	assert.NoError(err)
	defer f.Close()
	buf := bytes.NewBufferString("suffix")
	r := io.MultiReader(f, buf)

	b, err := NewBlob(r)
	assert.NoError(err)
	cb2 := b.(compoundBlob)

	assert.Equal(cb2.Len(), cb1.Len()+uint64(6))
	assert.Equal(3, len(cb1.blobs))
	assert.Equal(len(cb1.blobs), len(cb2.blobs))
	assert.Equal(cb1.blobs[0].Ref(), cb2.blobs[0].Ref())
	assert.Equal(cb1.blobs[1].Ref(), cb2.blobs[1].Ref())
	assert.NotEqual(cb1.blobs[2].Ref(), cb2.blobs[2].Ref())
}
