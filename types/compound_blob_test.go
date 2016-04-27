package types

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getTestCompoundBlob(datas ...string) compoundBlob {
	tuples := make([]metaTuple, len(datas))
	for i, s := range datas {
		b := NewBlob(bytes.NewBufferString(s))
		tuples[i] = newMetaTuple(Number(len(s)), b, Ref{}, uint64(len(s)))
	}
	return newCompoundBlob(tuples, nil)
}

func getRandomReader() io.ReadSeeker {
	length := int(5e5)
	s := rand.NewSource(42)
	buff := make([]byte, length)
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
	return NewBlob(r).(compoundBlob)
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

	test := func(b compoundBlob) {
		bs, err := ioutil.ReadAll(b.Reader())
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
	}

	cb := getTestCompoundBlob("hello", "world")
	test(cb)

	vs := NewTestValueStore()
	test(vs.ReadValue(vs.WriteValue(cb).TargetRef()).(compoundBlob))
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
	assert.Equal(int64(3), n)

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
	vs := NewTestValueStore()

	cb := getTestCompoundBlob("hello", "world")
	assert.Equal(2, len(cb.Chunks()))

	bl1 := newBlobLeaf([]byte("hello"))
	bl2 := newBlobLeaf([]byte("world"))
	cb = newCompoundBlob([]metaTuple{
		newMetaTuple(Number(5), bl1, Ref{}, 5),
		newMetaTuple(Number(10), bl2, Ref{}, 5),
	}, vs)
	assert.Equal(2, len(cb.Chunks()))
}

func TestCompoundBlobSameChunksWithPrefix(t *testing.T) {
	assert := assert.New(t)

	cb1 := getRandomBlob(t)

	// Load same file again but prepend some data... all but the first chunk should stay the same
	rr := getRandomReader()
	buf := bytes.NewBufferString("prefix")
	r := io.MultiReader(buf, rr)

	cb2 := NewBlob(r).(compoundBlob)

	// cb1: chunks 2
	//   chunks 21 - only first chunk is different
	//   chunks 31
	// cb2: chunks 2
	//   chunks 21
	//   chunks 31

	assert.Equal(cb2.Len(), cb1.Len()+uint64(6))
	assert.Equal(2, len(cb1.tuples))
	assert.Equal(2, len(cb2.tuples))
	assert.NotEqual(cb1.tuples[0].ChildRef(), cb2.tuples[0].ChildRef())
	assert.Equal(cb1.tuples[1].ChildRef(), cb2.tuples[1].ChildRef())

	tuples1 := cb1.tuples[0].child.(compoundBlob).tuples
	tuples2 := cb2.tuples[0].child.(compoundBlob).tuples
	assert.NotEqual(tuples1[0].ChildRef(), tuples2[0].ChildRef())
	assert.Equal(tuples1[1].ChildRef(), tuples2[1].ChildRef())
}

func TestCompoundBlobSameChunksWithSuffix(t *testing.T) {
	assert := assert.New(t)

	cb1 := getRandomBlob(t)

	// Load same file again but append some data... all but the last chunk should stay the same
	rr := getRandomReader()
	buf := bytes.NewBufferString("suffix")
	r := io.MultiReader(rr, buf)

	cb2 := NewBlob(r).(compoundBlob)

	// cb1: chunks 2
	//   chunks 21
	//   chunks 31
	// cb2: chunks 2
	//   chunks 21
	//   chunks 31 - only last chunk is different

	assert.Equal(cb2.Len(), cb1.Len()+uint64(6))
	assert.Equal(2, len(cb1.tuples))
	assert.Equal(len(cb1.tuples), len(cb2.tuples))
	assert.Equal(cb1.tuples[0].ChildRef(), cb2.tuples[0].ChildRef())
	assert.NotEqual(cb1.tuples[1].ChildRef(), cb2.tuples[1].ChildRef())

	tuples1 := cb1.tuples[1].child.(compoundBlob).tuples
	tuples2 := cb2.tuples[1].child.(compoundBlob).tuples
	assert.Equal(tuples1[0].ChildRef(), tuples2[0].ChildRef())
	assert.Equal(tuples1[len(tuples1)-2].ChildRef(), tuples2[len(tuples2)-2].ChildRef())
	assert.NotEqual(tuples1[len(tuples1)-1].ChildRef(), tuples2[len(tuples2)-1].ChildRef())
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
			printBlob(b.vr.ReadValue(t.ChildRef().TargetRef()).(Blob), indent)
		}
	}
}

func TestCompoundBlobType(t *testing.T) {
	assert := assert.New(t)

	cb := getTestCompoundBlob("hello", "world")
	assert.True(cb.Type().Equals(BlobType))
}
