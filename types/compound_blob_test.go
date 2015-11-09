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
	"github.com/attic-labs/noms/ref"
)

func getTestCompoundBlob(datas ...string) compoundBlob {
	blobs := make([]ref.Ref, len(datas))
	offsets := make([]uint64, len(datas))
	length := uint64(0)
	ms := chunks.NewMemoryStore()
	for i, s := range datas {
		b, _ := NewBlob(bytes.NewBufferString(s), ms)
		blobs[i] = WriteValue(b, ms)
		length += uint64(len(s))
		offsets[i] = length
	}
	return newCompoundBlob(offsets, blobs, ms)
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
	b, err := NewMemoryBlob(r)
	assert.NoError(t, err)
	return b.(compoundBlob)
}

func TestCompoundBlobReader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

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

	ref := WriteValue(cb, cb.cs.(chunks.ChunkStore))
	cb2 := ReadValue(ref, cb.cs)
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
	assert.Equal(2, len(cb.Chunks()))

	bl1 := newBlobLeaf([]byte("hello"))
	bl2 := newBlobLeaf([]byte("world"))
	cb = newCompoundBlob([]uint64{5, 10}, []ref.Ref{WriteValue(bl1, cs), WriteValue(bl2, cs)}, cs)
	assert.Equal(2, len(cb.Chunks()))
}

func TestCompoundBlobSameChunksWithPrefix(t *testing.T) {
	assert := assert.New(t)

	cb1 := getRandomBlob(t)

	// Load same file again but prepend some data... all but the first chunk should stay the same
	rr := getRandomReader()
	buf := bytes.NewBufferString("prefix")
	r := io.MultiReader(buf, rr)

	b, err := NewMemoryBlob(r)
	assert.NoError(err)
	cb2 := b.(compoundBlob)

	// cb1: chunks 2
	//   chunks 21 - only first chunk is different
	//   chunks 31
	// cb2: chunks 2
	//   chunks 21
	//   chunks 31

	assert.Equal(cb2.Len(), cb1.Len()+uint64(6))
	assert.Equal(2, len(cb1.chunks))
	assert.Equal(2, len(cb2.chunks))
	assert.NotEqual(cb1.chunks[0], cb2.chunks[0])
	assert.Equal(cb1.chunks[1], cb2.chunks[1])

	chunks1 := ReadValue(cb1.chunks[0], cb1.cs).(compoundBlob).chunks
	chunks2 := ReadValue(cb2.chunks[0], cb2.cs).(compoundBlob).chunks
	assert.NotEqual(chunks1[0], chunks2[0])
	assert.Equal(chunks1[1], chunks2[1])
}

func TestCompoundBlobSameChunksWithSuffix(t *testing.T) {
	assert := assert.New(t)

	cb1 := getRandomBlob(t)

	// Load same file again but append some data... all but the last chunk should stay the same
	rr := getRandomReader()
	buf := bytes.NewBufferString("suffix")
	r := io.MultiReader(rr, buf)

	b, err := NewMemoryBlob(r)
	assert.NoError(err)
	cb2 := b.(compoundBlob)

	// cb1: chunks 2
	//   chunks 21
	//   chunks 31
	// cb2: chunks 2
	//   chunks 21
	//   chunks 31 - only last chunk is different

	assert.Equal(cb2.Len(), cb1.Len()+uint64(6))
	assert.Equal(2, len(cb1.chunks))
	assert.Equal(len(cb1.chunks), len(cb2.chunks))
	assert.Equal(cb1.chunks[0], cb2.chunks[0])
	assert.NotEqual(cb1.chunks[1], cb2.chunks[1])

	chunks1 := ReadValue(cb1.chunks[1], cb1.cs).(compoundBlob).chunks
	chunks2 := ReadValue(cb2.chunks[1], cb2.cs).(compoundBlob).chunks
	assert.Equal(chunks1[0], chunks2[0])
	assert.Equal(chunks1[len(chunks1)-2], chunks2[len(chunks2)-2])
	assert.NotEqual(chunks1[len(chunks1)-1], chunks2[len(chunks2)-1])
}

func printBlob(b Blob, indent int) {
	indentString := strings.Repeat("|   ", indent)
	switch b := b.(type) {
	case blobLeaf:
		fmt.Printf("%sblobLeaf, len: %d\n", indentString, b.Len())
	case compoundBlob:
		fmt.Printf("%scompoundBlob, len: %d, chunks: %d\n", indentString, b.Len(), len(b.offsets))
		indent++
		for _, sb := range b.chunks {
			printBlob(ReadValue(sb, b.cs).(Blob), indent)
		}
	}
}

func TestCompoundBlobTypeRef(t *testing.T) {
	assert := assert.New(t)

	cb := getTestCompoundBlob("hello", "world")
	assert.True(cb.Type().Equals(MakePrimitiveTypeRef(BlobKind)))
}
