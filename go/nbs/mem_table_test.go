// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"sync"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
)

func TestMemTableAddHasGetChunk(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(1024)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	for _, c := range chunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	assertChunksInReader(chunks, mt, assert)

	for _, c := range chunks {
		assert.Equal(bytes.Compare(c, mt.get(computeAddr(c), &Stats{})), 0)
	}

	notPresent := []byte("nope")
	assert.False(mt.has(computeAddr(notPresent)))
	assert.Nil(mt.get(computeAddr(notPresent), &Stats{}))
}

func TestMemTableAddOverflowChunk(t *testing.T) {
	memTableSize := uint64(1024)

	assert := assert.New(t)
	big := make([]byte, memTableSize)
	little := []byte{0x01}
	{
		bigAddr := computeAddr(big)
		mt := newMemTable(memTableSize)
		assert.True(mt.addChunk(bigAddr, big))
		assert.True(mt.has(bigAddr))
		assert.False(mt.addChunk(computeAddr(little), little))
		assert.False(mt.has(computeAddr(little)))
	}

	{
		big := big[:memTableSize-1]
		bigAddr := computeAddr(big)
		mt := newMemTable(memTableSize)
		assert.True(mt.addChunk(bigAddr, big))
		assert.True(mt.has(bigAddr))
		assert.True(mt.addChunk(computeAddr(little), little))
		assert.True(mt.has(computeAddr(little)))
		other := []byte("o")
		assert.False(mt.addChunk(computeAddr(other), other))
		assert.False(mt.has(computeAddr(other)))
	}
}

func TestMemTableWrite(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(1024)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	for _, c := range chunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	td1, _ := buildTable(chunks[1:2])
	td2, _ := buildTable(chunks[2:])
	tr1 := newTableReader(parseTableIndex(td1), tableReaderAtFromBytes(td1), fileBlockSize)
	tr2 := newTableReader(parseTableIndex(td2), tableReaderAtFromBytes(td2), fileBlockSize)
	assert.True(tr1.has(computeAddr(chunks[1])))
	assert.True(tr2.has(computeAddr(chunks[2])))

	_, data, count := mt.write(chunkReaderGroup{tr1, tr2}, &Stats{})
	assert.Equal(uint32(1), count)

	outReader := newTableReader(parseTableIndex(data), tableReaderAtFromBytes(data), fileBlockSize)
	assert.True(outReader.has(computeAddr(chunks[0])))
	assert.False(outReader.has(computeAddr(chunks[1])))
	assert.False(outReader.has(computeAddr(chunks[2])))
}

type tableReaderAtAdapter struct {
	*bytes.Reader
}

func tableReaderAtFromBytes(b []byte) tableReaderAt {
	return tableReaderAtAdapter{bytes.NewReader(b)}
}

func (adapter tableReaderAtAdapter) ReadAtWithStats(p []byte, off int64, stats *Stats) (n int, err error) {
	return adapter.ReadAt(p, off)
}

func TestMemTableSnappyWriteOutOfLine(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(1024)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	for _, c := range chunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}
	mt.snapper = &outOfLineSnappy{[]bool{false, true, false}} // chunks[1] should trigger a panic

	assert.Panics(func() { mt.write(nil, &Stats{}) })
}

type outOfLineSnappy struct {
	policy []bool
}

func (o *outOfLineSnappy) Encode(dst, src []byte) []byte {
	outOfLine := false
	if len(o.policy) > 0 {
		outOfLine = o.policy[0]
		o.policy = o.policy[1:]
	}
	if outOfLine {
		return snappy.Encode(nil, src)
	}
	return snappy.Encode(dst, src)
}

type chunkReaderGroup []chunkReader

func (crg chunkReaderGroup) has(h addr) bool {
	for _, haver := range crg {
		if haver.has(h) {
			return true
		}
	}
	return false
}

func (crg chunkReaderGroup) get(h addr, stats *Stats) []byte {
	for _, haver := range crg {
		if data := haver.get(h, stats); data != nil {
			return data
		}
	}
	return nil
}

func (crg chunkReaderGroup) hasMany(addrs []hasRecord) (remaining bool) {
	for _, haver := range crg {
		if !haver.hasMany(addrs) {
			return false
		}
	}
	return true
}

func (crg chunkReaderGroup) getMany(reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup, stats *Stats) (remaining bool) {
	for _, haver := range crg {
		if !haver.getMany(reqs, foundChunks, wg, stats) {
			return false
		}
	}
	return true
}

func (crg chunkReaderGroup) count() (count uint32) {
	for _, haver := range crg {
		count += haver.count()
	}
	return
}

func (crg chunkReaderGroup) uncompressedLen() (data uint64) {
	for _, haver := range crg {
		data += haver.uncompressedLen()
	}
	return
}

func (crg chunkReaderGroup) extract(chunks chan<- extractRecord) {
	for _, haver := range crg {
		haver.extract(chunks)
	}
}
