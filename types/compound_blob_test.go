package types

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
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
	return newCompoundBlob(offsets, blobs, nil)
}

type randReader struct {
	s    rand.Source
	i    int
	size int
}

func (r *randReader) Read(p []byte) (n int, err error) {
	start := r.i
	for i := range p {
		if r.i == r.size {
			return r.i - start, io.EOF
		}
		p[i] = byte(r.s.Int63() & 0xff)
		r.i++
	}
	return len(p), nil
}

func getRandomReader() io.Reader {
	return &randReader{rand.NewSource(42), 0, 5e5}
}

func getRandomBlob(t *testing.T) compoundBlob {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	r := getRandomReader()
	b, err := NewBlob(r)
	assert.NoError(t, err)
	return b.(compoundBlob)
}

func TestCompoundBlobReader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	cb := getTestCompoundBlob("hello", "world")
	bs, err := ioutil.ReadAll(cb.Reader())
	assert.NoError(err)
	assert.Equal("helloworld", string(bs))

	ab := getRandomBlob(t)
	bs, err = ioutil.ReadAll(ab.Reader())
	assert.NoError(err)
	r := getRandomReader()
	bs2, err := ioutil.ReadAll(r)
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

	cb := newCompoundBlob([]uint64{2, 5}, []Future{futureFromValue(tb1), futureFromValue(tb2)}, nil)

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

	cb := newCompoundBlob([]uint64{2, 5}, []Future{futureFromValue(tb1), futureFromValue(tb2)}, nil)

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

	ab := getRandomBlob(t)
	assert.Equal(uint64(5e5), ab.Len())
}

func TestCompoundBlobChunks(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	cb := getTestCompoundBlob("hello", "world")
	assert.Equal(0, len(cb.Chunks()))

	bl1 := newBlobLeaf([]byte("hello"))
	blr1 := bl1.Ref()
	bl2 := newBlobLeaf([]byte("world"))
	cb = newCompoundBlob([]uint64{5, 10}, []Future{futureFromRef(blr1), futureFromValue(bl2)}, cs)
	assert.Equal(1, len(cb.Chunks()))
}

func TestCompoundBlobSameChunksWithPrefix(t *testing.T) {
	assert := assert.New(t)

	cb1 := getRandomBlob(t)

	// Load same file again but prepend some data... all but the first chunk should stay the same
	rr := getRandomReader()
	buf := bytes.NewBufferString("prefix")
	r := io.MultiReader(buf, rr)

	b, err := NewBlob(r)
	assert.NoError(err)
	cb2 := b.(compoundBlob)

	// cb1: chunks 2
	//   chunks 21 - only first chunk is different
	//   chunks 31
	// cb2: chunks 2
	//   chunks 21
	//   chunks 31

	assert.Equal(cb2.Len(), cb1.Len()+uint64(6))
	assert.Equal(2, len(cb1.futures))
	assert.Equal(2, len(cb2.futures))
	assert.NotEqual(cb1.futures[0].Ref(), cb2.futures[0].Ref())
	assert.Equal(cb1.futures[1].Ref(), cb2.futures[1].Ref())

	futures1 := cb1.futures[0].Deref(nil).(compoundBlob).futures
	futures2 := cb2.futures[0].Deref(nil).(compoundBlob).futures
	assert.NotEqual(futures1[0].Ref(), futures2[0].Ref())
	assert.Equal(futures1[1].Ref(), futures2[1].Ref())
}

func TestCompoundBlobSameChunksWithSuffix(t *testing.T) {
	assert := assert.New(t)

	cb1 := getRandomBlob(t)

	// Load same file again but append some data... all but the last chunk should stay the same
	rr := getRandomReader()
	buf := bytes.NewBufferString("suffix")
	r := io.MultiReader(rr, buf)

	b, err := NewBlob(r)
	assert.NoError(err)
	cb2 := b.(compoundBlob)

	// cb1: chunks 2
	//   chunks 21
	//   chunks 31
	// cb2: chunks 2
	//   chunks 21
	//   chunks 31 - only last chunk is different

	assert.Equal(cb2.Len(), cb1.Len()+uint64(6))
	assert.Equal(2, len(cb1.futures))
	assert.Equal(len(cb1.futures), len(cb2.futures))
	assert.Equal(cb1.futures[0].Ref(), cb2.futures[0].Ref())
	assert.NotEqual(cb1.futures[1].Ref(), cb2.futures[1].Ref())

	futures1 := cb1.futures[1].Deref(nil).(compoundBlob).futures
	futures2 := cb2.futures[1].Deref(nil).(compoundBlob).futures
	assert.Equal(futures1[0].Ref(), futures2[0].Ref())
	assert.Equal(futures1[len(futures1)-2].Ref(), futures2[len(futures2)-2].Ref())
	assert.NotEqual(futures1[len(futures1)-1].Ref(), futures2[len(futures2)-1].Ref())
}

func printBlob(b Blob, indent int) {
	indentString := strings.Repeat("|   ", indent)
	switch b := b.(type) {
	case blobLeaf:
		fmt.Printf("%sblobLeaf, len: %d\n", indentString, b.Len())
	case compoundBlob:
		fmt.Printf("%scompoundBlob, len: %d, chunks: %d\n", indentString, b.Len(), len(b.offsets))
		indent++
		for _, sb := range b.futures {
			printBlob(sb.Deref(b.cs).(Blob), indent)
		}
	}
}
