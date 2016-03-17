package types

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func AssertSymEq(assert *assert.Assertions, a, b Value) {
	assert.True(a.Equals(b))
	assert.True(b.Equals(a))
}

func AssertSymNe(assert *assert.Assertions, a, b Value) {
	assert.False(a.Equals(b))
	assert.False(b.Equals(a))
}

func TestBlobLen(t *testing.T) {
	assert := assert.New(t)
	b := NewBlob(&bytes.Buffer{})
	assert.Equal(uint64(0), b.Len())
	b = NewBlob(bytes.NewBuffer([]byte{0x01}))
	assert.Equal(uint64(1), b.Len())
}

func TestBlobEquals(t *testing.T) {
	assert := assert.New(t)
	b1 := NewBlob(bytes.NewBuffer([]byte{0x01}))
	b11 := b1
	b12 := NewBlob(bytes.NewBuffer([]byte{0x01}))
	b2 := NewBlob(bytes.NewBuffer([]byte{0x02}))
	b3 := NewBlob(bytes.NewBuffer([]byte{0x02, 0x03}))
	AssertSymEq(assert, b1, b11)
	AssertSymEq(assert, b1, b12)
	AssertSymNe(assert, b1, b2)
	AssertSymNe(assert, b2, b3)
	AssertSymNe(assert, b1, Int32(1))
}

type testReader struct {
	readCount int
	buf       *bytes.Buffer
}

func (r *testReader) Read(p []byte) (n int, err error) {
	r.readCount++

	switch r.readCount {
	case 1:
		for i := 0; i < len(p); i++ {
			p[i] = 0x01
		}
		io.Copy(r.buf, bytes.NewReader(p))
		return len(p), nil
	case 2:
		p[0] = 0x02
		r.buf.WriteByte(p[0])
		return 1, io.EOF
	default:
		return 0, io.EOF
	}
}

func TestBlobFromReaderThatReturnsDataAndError(t *testing.T) {
	// See issue #264.
	// This tests the case of building a Blob from a reader who returns both data and an error for the final Read() call.
	assert := assert.New(t)
	tr := &testReader{buf: &bytes.Buffer{}}

	b := NewBlob(tr)

	actual := &bytes.Buffer{}
	io.Copy(actual, b.Reader())

	assert.True(bytes.Equal(actual.Bytes(), tr.buf.Bytes()))
	assert.Equal(byte(2), actual.Bytes()[len(actual.Bytes())-1])
}

func TestBlobChunkingSameAsJavascript(t *testing.T) {
	assert := assert.New(t)
	b := NewBlob(bytes.NewBuffer([]byte{
		141,
		136,
		71,
		250,
		17,
		60,
		107,
		206,
		213,
		48,
		207,
		226,
		217,
		100,
		115,
	}))

	assert.Equal(b.Ref().String(), "sha1-fc30f237649464078574bc46b90c842179b4fa18")
}
