// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"

	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
)

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

func newBlobTestSuite(size uint, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int) *blobTestSuite {
	length := 1 << size
	buff := randomBuff(size)
	blob := NewBlob(bytes.NewReader(buff))
	return &blobTestSuite{
		collectionTestSuite: collectionTestSuite{
			col:                    blob,
			expectType:             BlobType,
			expectLen:              uint64(length),
			expectChunkCount:       expectChunkCount,
			expectPrependChunkDiff: expectPrependChunkDiff,
			expectAppendChunkDiff:  expectAppendChunkDiff,
			validate: func(v2 Collection) bool {
				b2 := v2.(Blob)
				outBuff := &bytes.Buffer{}
				b2.Copy(outBuff)
				return bytes.Compare(outBuff.Bytes(), buff) == 0
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

func TestBlobSuite4K(t *testing.T) {
	suite.Run(t, newBlobTestSuite(12, 0, 0, 0))
}

func TestBlobSuite64K(t *testing.T) {
	suite.Run(t, newBlobTestSuite(16, 2, 2, 2))
}

func TestBlobSuite256K(t *testing.T) {
	suite.Run(t, newBlobTestSuite(18, 6, 2, 2))
}

func TestBlobSuite1M(t *testing.T) {
	suite.Run(t, newBlobTestSuite(20, 23, 2, 2))
}

// Checks the first 1/2 of the bytes, then 1/2 of the remainder, then 1/2 of the remainder, etc...
func (suite *blobTestSuite) TestRandomRead() {
	buffReader := bytes.NewReader(suite.buff)
	blobReader := suite.col.(Blob).Reader()

	readByteRange := func(r io.ReadSeeker, start, rel, count int64) []byte {
		bytes := make([]byte, count)
		n, err := r.Seek(start, 0)
		suite.NoError(err)
		suite.Equal(start, n)
		n2, err := r.Seek(rel, 1)
		suite.NoError(err)
		suite.Equal(start+rel, n2)
		n3, err := io.ReadFull(r, bytes)
		suite.NoError(err)
		suite.Equal(int(count), n3)
		return bytes
	}

	readByteRangeFromEnd := func(r io.ReadSeeker, length, offset, count int64) []byte {
		bytes := make([]byte, count)
		n, err := r.Seek(offset, 2)
		suite.NoError(err)
		suite.Equal(length+offset, n)
		n2, err := io.ReadFull(r, bytes)
		suite.NoError(err)
		suite.Equal(int(count), n2)
		return bytes
	}

	checkByteRange := func(start, rel, count int64) {
		expect := readByteRange(buffReader, start, rel, count)
		actual := readByteRange(blobReader, start, rel, count)
		suite.Equal(expect, actual)
	}

	checkByteRangeFromEnd := func(length, offset, count int64) {
		expect := readByteRangeFromEnd(buffReader, length, offset, count)
		actual := readByteRangeFromEnd(blobReader, length, offset, count)
		suite.Equal(expect, actual)
	}

	length := int64(len(suite.buff))
	start := int64(0)
	count := int64(length / 2)
	for count > 2 {
		checkByteRange(start, 0, count)
		checkByteRange(0, start, count)
		checkByteRange(start/2, start-(start/2), count)
		checkByteRangeFromEnd(length, start-length, count)
		start = start + count
		count = (length - start) / 2
	}
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

func TestBlobSplice(t *testing.T) {
	assert := assert.New(t)

	blob := NewEmptyBlob()
	buf := new(bytes.Buffer)

	blob = blob.Splice(0, 0, []byte("I'll do anything"))
	buf.Reset()
	buf.ReadFrom(blob.Reader())
	assert.Equal(buf.String(), "I'll do anything")

	blob = blob.Splice(16, 0, []byte(" for arv"))
	buf.Reset()
	buf.ReadFrom(blob.Reader())
	assert.Equal(buf.String(), "I'll do anything for arv")

	blob = blob.Splice(0, 0, []byte("Yes, "))
	buf.Reset()
	buf.ReadFrom(blob.Reader())
	assert.Equal(buf.String(), "Yes, I'll do anything for arv")

	blob = blob.Splice(5, 20, []byte("it's hard to satisfy"))
	buf.Reset()
	buf.ReadFrom(blob.Reader())
	assert.Equal(buf.String(), "Yes, it's hard to satisfy arv")
}

func TestBlobConcat(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()
	reload := func(b Blob) Blob {
		return vs.ReadValue(vs.WriteValue(b).TargetHash()).(Blob)
	}

	split := func(b Blob, at int64) (Blob, Blob) {
		read1, read2 := b.Reader(), b.Reader()
		b1 := NewBlob(&io.LimitedReader{read1, at})
		read2.Seek(at, 0)
		b2 := NewBlob(read2)
		return reload(b1), reload(b2)
	}

	// Random 1MB Blob.
	// Note that List.Concat is exhaustively tested, don't worry here.
	r := rand.New(rand.NewSource(0))
	b := NewBlob(&io.LimitedReader{r, 1e6})
	b = reload(b)

	b1 := NewEmptyBlob().Concat(b)
	assert.True(b.Equals(b1))

	b2 := b.Concat(NewEmptyBlob())
	assert.True(b.Equals(b2))

	b3, b4 := split(b, 10)
	assert.True(b.Equals(b3.Concat(b4)))

	b5, b6 := split(b, 1e6-10)
	assert.True(b.Equals(b5.Concat(b6)))

	b7, b8 := split(b, 1e6/2)
	assert.True(b.Equals(b7.Concat(b8)))
}

func TestBlobNewParallel(t *testing.T) {
	assert := assert.New(t)

	readAll := func(b Blob) []byte {
		data, err := ioutil.ReadAll(b.Reader())
		assert.NoError(err)
		return data
	}

	b := NewBlob()
	assert.True(b.Len() == 0)

	b = NewBlob(strings.NewReader("abc"))
	assert.Equal("abc", string(readAll(b)))

	b = NewBlob(strings.NewReader("abc"), strings.NewReader("def"))
	assert.Equal("abcdef", string(readAll(b)))

	p, size := 100, 1024
	r := rand.New(rand.NewSource(0))
	data := make([]byte, p*size)
	_, err := r.Read(data)
	assert.NoError(err)

	readers := make([]io.Reader, p)
	for i := range readers {
		readers[i] = bytes.NewBuffer(data[i*size : (i+1)*size])
	}

	b = NewBlob(readers...)
	assert.Equal(data, readAll(b))
}

func TestStreamingParallelBlob(t *testing.T) {
	assert := assert.New(t)

	buff := randomBuff(1 << 26 /* 64MB */)
	chunks := 4
	readers := make([]io.Reader, chunks)
	chunkSize := len(buff) / chunks

	for i := 0; i < len(readers); i++ {
		readers[i] = bytes.NewReader(buff[i*chunkSize : (i+1)*chunkSize])
	}

	vs := newTestValueStore()
	blob := NewStreamingBlob(vs, readers...)
	outBuff := &bytes.Buffer{}
	blob.Copy(outBuff)
	assert.True(bytes.Compare(buff, outBuff.Bytes()) == 0)
}
