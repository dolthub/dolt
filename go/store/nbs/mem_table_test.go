// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/golang/snappy"
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/stretchr/testify/assert"
)

func TestWriteChunks(t *testing.T) {
	chunks := []chunks.Chunk{
		types.EncodeValue(types.String("Call me Ishmael. Some years ago—never mind how long precisely—having little or no money in my purse, ")),
		types.EncodeValue(types.String("and nothing particular to interest me on shore, I thought I would sail about a little and see the watery ")),
		types.EncodeValue(types.String("part of the world. It is a way I have of driving off the spleen and regulating the ")),
		types.EncodeValue(types.String("circulation. Whenever I find myself growing grim about the mouth; whenever it is a damp, drizzly ")),
		types.EncodeValue(types.String("November in my soul; whenever I find myself involuntarily pausing before coffin warehouses, and bringing ")),
		types.EncodeValue(types.String("funeral I meet; and especially whenever my hypos get such an upper hand of me, that it requires ")),
		types.EncodeValue(types.String("a strong moral principle to prevent me from deliberately stepping into the street, and methodically ")),
		types.EncodeValue(types.String("knocking people’s hats off—then, I account it high time to get to sea as soon as I can.")),
	}

	name, data, err := WriteChunks(chunks)
	if err != nil {
		t.Error(err)
	}

	dir, err := ioutil.TempDir("", "write_chunks_test")
	if err != nil {
		t.Error(err)
	}

	err = ioutil.WriteFile(dir+name, data, os.ModePerm)
	if err != nil {
		t.Error(err)
	}
}

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
		data, err := mt.get(context.Background(), computeAddr(c), &Stats{})
		assert.NoError(err)
		assert.Equal(bytes.Compare(c, data), 0)
	}

	notPresent := []byte("nope")
	assert.False(mt.has(computeAddr(notPresent)))
	assert.Nil(mt.get(context.Background(), computeAddr(notPresent), &Stats{}))
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
	ti1, err := parseTableIndex(td1)
	assert.NoError(err)
	tr1 := newTableReader(ti1, tableReaderAtFromBytes(td1), fileBlockSize)
	assert.True(tr1.has(computeAddr(chunks[1])))

	td2, _ := buildTable(chunks[2:])
	ti2, err := parseTableIndex(td2)
	assert.NoError(err)
	tr2 := newTableReader(ti2, tableReaderAtFromBytes(td2), fileBlockSize)
	assert.True(tr2.has(computeAddr(chunks[2])))

	_, data, count := mt.write(chunkReaderGroup{tr1, tr2}, &Stats{})
	assert.Equal(uint32(1), count)

	ti, err := parseTableIndex(data)
	assert.NoError(err)
	outReader := newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
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

func (adapter tableReaderAtAdapter) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
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

func (crg chunkReaderGroup) has(h addr) (bool, error) {
	for _, haver := range crg {
		ok, err := haver.has(h)

		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (crg chunkReaderGroup) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	for _, haver := range crg {
		if data, err := haver.get(ctx, h, stats); err != nil {
			return nil, err
		} else if data != nil {
			return data, nil
		}
	}

	return nil, nil
}

func (crg chunkReaderGroup) hasMany(addrs []hasRecord) (bool, error) {
	for _, haver := range crg {
		remaining, err := haver.hasMany(addrs)

		if err != nil {
			return false, err
		}

		if !remaining {
			return false, nil
		}
	}
	return true, nil
}

func (crg chunkReaderGroup) getMany(ctx context.Context, reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup, ae *AtomicError, stats *Stats) bool {
	for _, haver := range crg {
		remaining := haver.getMany(ctx, reqs, foundChunks, wg, ae, stats)

		if !remaining {
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

func (crg chunkReaderGroup) extract(ctx context.Context, chunks chan<- extractRecord) error {
	for _, haver := range crg {
		err := haver.extract(ctx, chunks)

		if err != nil {
			return err
		}
	}

	return nil
}
