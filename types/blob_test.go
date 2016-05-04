package types

import (
	"bytes"
	"io"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// IMPORTANT: These tests and in particular the hash of the values should stay in sync with the
// corresponding tests in js

type countingReader struct {
	last uint32
	val  uint32
	bc   uint8
}

func newCountingReader() *countingReader {
	return &countingReader{0, 0, 4}
}

func (rr *countingReader) next() byte {
	if rr.bc == 0 {
		rr.last = rr.last + 1
		rr.val = rr.last
		rr.bc = 4
	}

	retval := byte(uint64(rr.val) & 0xff)
	rr.bc--
	rr.val = rr.val >> 8
	return retval
}

func (rr *countingReader) Read(p []byte) (n int, err error) {
	for i := 0; i < len(p); i++ {
		p[i] = rr.next()
	}
	return len(p), nil
}

func randomBuff(powOfTwo uint) []byte {
	length := 1 << powOfTwo
	rr := newCountingReader()
	buff := make([]byte, length)
	rr.Read(buff)
	return buff
}

type blobTestSuite struct {
	collectionTestSuite
	buff []byte
}

func newBlobTestSuite(size uint, expectRefStr string, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int) *blobTestSuite {
	length := 1 << size
	buff := randomBuff(size)
	blob := NewBlob(bytes.NewReader(buff))
	return &blobTestSuite{
		collectionTestSuite: collectionTestSuite{
			col:                    blob,
			expectType:             BlobType,
			expectLen:              uint64(length),
			expectRef:              expectRefStr,
			expectChunkCount:       expectChunkCount,
			expectPrependChunkDiff: expectPrependChunkDiff,
			expectAppendChunkDiff:  expectAppendChunkDiff,
			validate: func(v2 Collection) bool {
				b2 := v2.(Blob)
				out := make([]byte, length)
				io.ReadFull(b2.Reader(), out)
				return bytes.Compare(out, buff) == 0
			},
			prependOne: func() Collection {
				dup := make([]byte, length+1)
				dup[0] = 0
				copy(dup[1:], buff)
				return NewBlob(bytes.NewReader(dup))
			},
			appendOne: func() Collection {
				dup := make([]byte, length+1)
				copy(dup, buff)
				dup[len(dup)-1] = 0
				return NewBlob(bytes.NewReader(dup))
			},
		},
		buff: buff,
	}
}

func TestBlobSuite1K(t *testing.T) {
	suite.Run(t, newBlobTestSuite(10, "sha1-28e2f80d426cdb8bdbb347d00050b4d3fcb644a8", 3, 2, 2))
}

func TestBlobSuite4K(t *testing.T) {
	suite.Run(t, newBlobTestSuite(12, "sha1-5b2413e80d091f8b978ce927767e19e5655ac1a0", 9, 2, 2))
}

func TestBlobSuite16K(t *testing.T) {
	suite.Run(t, newBlobTestSuite(14, "sha1-666ebeed9cbcbcb2da71c0bd578c7266a5abf9b2", 33, 2, 2))
}

func TestBlobSuite64K(t *testing.T) {
	suite.Run(t, newBlobTestSuite(16, "sha1-a521ed352977cc544c2c98ac1a458f19fc551dce", 4, 2, 2))
}

func TestBlobSuite256K(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	suite.Run(t, newBlobTestSuite(18, "sha1-4692cb536901b70e66109191b0091cfb2ed32eea", 2, 15, 2))
}

// Checks the first 1/2 of the bytes, then 1/2 of the remainder, then 1/2 of the remainder, etc...
func (suite *blobTestSuite) TestRandomRead() {
	buffReader := bytes.NewReader(suite.buff)
	blobReader := suite.col.(Blob).Reader()

	readByteRange := func(r io.ReadSeeker, start int64, count int64) []byte {
		bytes := make([]byte, count)
		n, err := r.Seek(start, 0)
		suite.NoError(err)
		suite.Equal(start, n)
		n2, err := io.ReadFull(r, bytes)
		suite.NoError(err)
		suite.Equal(int(count), n2)
		return bytes
	}

	checkByteRange := func(start int64, count int64) {
		expect := readByteRange(buffReader, start, count)
		actual := readByteRange(blobReader, start, count)
		suite.Equal(expect, actual)
	}

	length := int64(len(suite.buff))
	start := int64(0)
	count := int64(length / 2)
	for count > 2 {
		checkByteRange(start, count)
		start = start + count
		count = (length - start) / 2
	}
}

func chunkDiffCount(c1 []Ref, c2 []Ref) int {
	count := 0
	refs := make(map[ref.Ref]int)

	for _, r := range c1 {
		refs[r.TargetRef()]++
	}

	for _, r := range c2 {
		if c, ok := refs[r.TargetRef()]; ok {
			if c == 1 {
				delete(refs, r.TargetRef())
			} else {
				refs[r.TargetRef()] = c - 1
			}
		} else {
			count++
		}
	}

	count += len(refs)
	return count
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
