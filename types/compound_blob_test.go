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
	tuples := make([]metaTuple, len(datas))
	length := uint64(0)
	ms := chunks.NewMemoryStore()
	for i, s := range datas {
		b := NewBlob(bytes.NewBufferString(s), ms)
		length += uint64(len(s))
		tuples[i] = metaTuple{WriteValue(b, ms), UInt64(length)}
	}
	return newCompoundBlob(tuples, ms)
}

func getRandomReader() io.ReadSeeker {
	length := int(5e5)
	s := rand.NewSource(42)
	buff := make([]byte, 5e5, 5e5)
	for i := 0; i < length; i++ {
		buff[i] = byte(s.Int63() & 0xff)
	}

	return bytes.NewReader(buff)
}

func getRandomBlob(t *testing.T) compoundBlob {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	r := getRandomReader()
	return NewMemoryBlob(r).(compoundBlob)
}

func testByteRange(assert *assert.Assertions, offset, length int64, expect io.ReadSeeker, actual io.ReadSeeker) {
	n, err := expect.Seek(offset, 0)
	assert.Equal(offset, n)
	assert.NoError(err)

	b1 := &bytes.Buffer{}
	n, err = io.CopyN(b1, expect, length)
	assert.Equal(length, n)
	assert.NoError(err)

	n, err = actual.Seek(offset, 0)
	assert.Equal(offset, n)
	assert.NoError(err)

	b2 := &bytes.Buffer{}
	n, err = io.CopyN(b2, actual, length)
	assert.Equal(length, n)
	assert.NoError(err)

	assert.Equal(b1.Bytes(), b2.Bytes())
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
	testByteRange(assert, 200453, 100232, r, ab.Reader())
	testByteRange(assert, 100, 10, r, ab.Reader())
	testByteRange(assert, 2340, 2630, r, ab.Reader())
	testByteRange(assert, 432423, 50000, r, ab.Reader())
	testByteRange(assert, 1, 10, r, ab.Reader())

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

func TestCompoundBlobReaderSeek(t *testing.T) {
	assert := assert.New(t)

	cb := getTestCompoundBlob("hi", "bye")

	r := cb.Reader()
	_, err := r.Seek(0, 4)
	assert.Error(err)

	_, err = r.Seek(-1, 0)
	assert.Error(err)

	p := []byte{0}

	n, err := r.Seek(3, 0)
	assert.NoError(err)
	assert.Equal(int64(3), n)

	n2, err := r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n2)
	assert.Equal("y", string(p))

	n, err = r.Seek(-1, 1)
	assert.NoError(err)
	// assert.Equal(int64(3), n)

	n2, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n2)
	assert.Equal("y", string(p))

	n, err = r.Seek(-5, 2)
	assert.NoError(err)
	assert.Equal(int64(0), n)

	n2, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n2)
	assert.Equal("h", string(p))

	n, err = r.Seek(100, 0)
	assert.NoError(err)
	assert.Equal(int64(100), n)

	n2, err = r.Read(p)
	assert.Equal(io.EOF, err)
	assert.Equal(0, n2)

	n, err = r.Seek(-99, 1)
	assert.NoError(err)
	assert.Equal(int64(1), n)

	n2, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n2)
	assert.Equal("i", string(p))

	n2, err = r.Read(p)
	assert.NoError(err)
	assert.Equal(1, n2)
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
	assert.Equal(2, len(cb.Chunks()))

	bl1 := newBlobLeaf([]byte("hello"))
	bl2 := newBlobLeaf([]byte("world"))
	cb = newCompoundBlob([]metaTuple{{WriteValue(bl1, cs), UInt64(uint64(5))}, {WriteValue(bl2, cs), UInt64(uint64(10))}}, cs)
	assert.Equal(2, len(cb.Chunks()))
}

func TestCompoundBlobSameChunksWithPrefix(t *testing.T) {
	assert := assert.New(t)

	cb1 := getRandomBlob(t)

	// Load same file again but prepend some data... all but the first chunk should stay the same
	rr := getRandomReader()
	buf := bytes.NewBufferString("prefix")
	r := io.MultiReader(buf, rr)

	cb2 := NewMemoryBlob(r).(compoundBlob)

	// cb1: chunks 2
	//   chunks 21 - only first chunk is different
	//   chunks 31
	// cb2: chunks 2
	//   chunks 21
	//   chunks 31

	assert.Equal(cb2.Len(), cb1.Len()+uint64(6))
	assert.Equal(2, len(cb1.tuples))
	assert.Equal(2, len(cb2.tuples))
	assert.NotEqual(cb1.tuples[0].ref, cb2.tuples[0].ref)
	assert.Equal(cb1.tuples[1].ref, cb2.tuples[1].ref)

	tuples1 := ReadValue(cb1.tuples[0].ref, cb1.cs).(compoundBlob).tuples
	tuples2 := ReadValue(cb2.tuples[0].ref, cb2.cs).(compoundBlob).tuples
	assert.NotEqual(tuples1[0].ref, tuples2[0].ref)
	assert.Equal(tuples1[1].ref, tuples2[1].ref)
}

func TestCompoundBlobSameChunksWithSuffix(t *testing.T) {
	assert := assert.New(t)

	cb1 := getRandomBlob(t)

	// Load same file again but append some data... all but the last chunk should stay the same
	rr := getRandomReader()
	buf := bytes.NewBufferString("suffix")
	r := io.MultiReader(rr, buf)

	cb2 := NewMemoryBlob(r).(compoundBlob)

	// cb1: chunks 2
	//   chunks 21
	//   chunks 31
	// cb2: chunks 2
	//   chunks 21
	//   chunks 31 - only last chunk is different

	assert.Equal(cb2.Len(), cb1.Len()+uint64(6))
	assert.Equal(2, len(cb1.tuples))
	assert.Equal(len(cb1.tuples), len(cb2.tuples))
	assert.Equal(cb1.tuples[0].ref, cb2.tuples[0].ref)
	assert.NotEqual(cb1.tuples[1].ref, cb2.tuples[1].ref)

	tuples1 := ReadValue(cb1.tuples[1].ref, cb1.cs).(compoundBlob).tuples
	tuples2 := ReadValue(cb2.tuples[1].ref, cb2.cs).(compoundBlob).tuples
	assert.Equal(tuples1[0].ref, tuples2[0].ref)
	assert.Equal(tuples1[len(tuples1)-2].ref, tuples2[len(tuples2)-2].ref)
	assert.NotEqual(tuples1[len(tuples1)-1].ref, tuples2[len(tuples2)-1].ref)
}

func printBlob(b Blob, indent int) {
	indentString := strings.Repeat("|   ", indent)
	switch b := b.(type) {
	case blobLeaf:
		fmt.Printf("%sblobLeaf, len: %d\n", indentString, b.Len())
	case compoundBlob:
		fmt.Printf("%scompoundBlob, len: %d, chunks: %d\n", indentString, b.Len(), len(b.tuples))
		indent++
		for _, t := range b.tuples {
			printBlob(ReadValue(t.ref, b.cs).(Blob), indent)
		}
	}
}

func TestCompoundBlobType(t *testing.T) {
	assert := assert.New(t)

	cb := getTestCompoundBlob("hello", "world")
	assert.True(cb.Type().Equals(MakeCompoundType(MetaSequenceKind, MakePrimitiveType(BlobKind))))
}
