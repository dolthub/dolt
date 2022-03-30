// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/types"
)

var testMDChunks = []chunks.Chunk{
	mustChunk(types.EncodeValue(types.String("Call me Ishmael. Some years ago—never mind how long precisely—having little or no money in my purse, "), types.Format_Default)),
	mustChunk(types.EncodeValue(types.String("and nothing particular to interest me on shore, I thought I would sail about a little and see the watery "), types.Format_Default)),
	mustChunk(types.EncodeValue(types.String("part of the world. It is a way I have of driving off the spleen and regulating the "), types.Format_Default)),
	mustChunk(types.EncodeValue(types.String("circulation. Whenever I find myself growing grim about the mouth; whenever it is a damp, drizzly "), types.Format_Default)),
	mustChunk(types.EncodeValue(types.String("November in my soul; whenever I find myself involuntarily pausing before coffin warehouses, and bringing "), types.Format_Default)),
	mustChunk(types.EncodeValue(types.String("funeral I meet; and especially whenever my hypos get such an upper hand of me, that it requires "), types.Format_Default)),
	mustChunk(types.EncodeValue(types.String("a strong moral principle to prevent me from deliberately stepping into the street, and methodically "), types.Format_Default)),
	mustChunk(types.EncodeValue(types.String("knocking people’s hats off—then, I account it high time to get to sea as soon as I can."), types.Format_Default)),
}

var testMDChunksSize uint64

func init() {
	for _, chunk := range testMDChunks {
		testMDChunksSize += uint64(len(chunk.Data()))
	}
}

func mustChunk(chunk chunks.Chunk, err error) chunks.Chunk {
	d.PanicIfError(err)
	return chunk
}

func TestWriteChunks(t *testing.T) {
	name, data, err := WriteChunks(testMDChunks)
	if err != nil {
		t.Error(err)
	}

	dir, err := os.MkdirTemp("", "write_chunks_test")
	if err != nil {
		t.Error(err)
	}

	err = os.WriteFile(dir+name, data, os.ModePerm)
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
		require.NoError(t, err)
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

	td1, _, err := buildTable(chunks[1:2])
	require.NoError(t, err)
	ti1, err := parseTableIndexByCopy(td1, &noopQuotaProvider{})
	require.NoError(t, err)
	tr1, err := newTableReader(ti1, tableReaderAtFromBytes(td1), fileBlockSize)
	require.NoError(t, err)
	assert.True(tr1.has(computeAddr(chunks[1])))

	td2, _, err := buildTable(chunks[2:])
	require.NoError(t, err)
	ti2, err := parseTableIndexByCopy(td2, &noopQuotaProvider{})
	require.NoError(t, err)
	tr2, err := newTableReader(ti2, tableReaderAtFromBytes(td2), fileBlockSize)
	require.NoError(t, err)
	assert.True(tr2.has(computeAddr(chunks[2])))

	_, data, count, err := mt.write(chunkReaderGroup{tr1, tr2}, &Stats{})
	require.NoError(t, err)
	assert.Equal(uint32(1), count)

	ti, err := parseTableIndexByCopy(data, &noopQuotaProvider{})
	require.NoError(t, err)
	outReader, err := newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
	require.NoError(t, err)
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

func (crg chunkReaderGroup) getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), stats *Stats) (bool, error) {
	for _, haver := range crg {
		remaining, err := haver.getMany(ctx, eg, reqs, found, stats)
		if err != nil {
			return true, err
		}
		if !remaining {
			return false, nil
		}
	}
	return true, nil
}

func (crg chunkReaderGroup) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (bool, error) {
	for _, haver := range crg {
		remaining, err := haver.getManyCompressed(ctx, eg, reqs, found, stats)
		if err != nil {
			return true, err
		}
		if !remaining {
			return false, nil
		}
	}
	return true, nil
}

func (crg chunkReaderGroup) count() (count uint32, err error) {
	for _, haver := range crg {
		count += mustUint32(haver.count())
	}
	return
}

func (crg chunkReaderGroup) uncompressedLen() (data uint64, err error) {
	for _, haver := range crg {
		data += mustUint64(haver.uncompressedLen())
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

func (crg chunkReaderGroup) Close() error {
	var firstErr error
	for _, c := range crg {
		err := c.Close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
