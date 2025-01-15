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
	"io"
	"os"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
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
		assert.Equal(mt.addChunk(computeAddr(c), c), chunkAdded)
	}

	assertChunksInReader(chunks, mt, assert)

	for _, c := range chunks {
		data, _, err := mt.get(context.Background(), computeAddr(c), nil, &Stats{})
		require.NoError(t, err)
		assert.Equal(bytes.Compare(c, data), 0)
	}

	notPresent := []byte("nope")
	assert.False(mt.has(computeAddr(notPresent), nil))
	assert.Nil(mt.get(context.Background(), computeAddr(notPresent), nil, &Stats{}))
}

func TestMemTableAddOverflowChunk(t *testing.T) {
	memTableSize := uint64(1024)

	assert := assert.New(t)
	big := make([]byte, memTableSize)
	little := []byte{0x01}
	{
		bigAddr := computeAddr(big)
		mt := newMemTable(memTableSize)
		assert.Equal(mt.addChunk(bigAddr, big), chunkAdded)
		assert.True(mt.has(bigAddr, nil))
		assert.Equal(mt.addChunk(computeAddr(little), little), chunkNotAdded)
		assert.False(mt.has(computeAddr(little), nil))
	}

	{
		big := big[:memTableSize-1]
		bigAddr := computeAddr(big)
		mt := newMemTable(memTableSize)
		assert.Equal(mt.addChunk(bigAddr, big), chunkAdded)
		assert.True(mt.has(bigAddr, nil))
		assert.Equal(mt.addChunk(computeAddr(little), little), chunkAdded)
		assert.True(mt.has(computeAddr(little), nil))
		other := []byte("o")
		assert.Equal(mt.addChunk(computeAddr(other), other), chunkNotAdded)
		assert.False(mt.has(computeAddr(other), nil))
	}
}

func TestMemTableWrite(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	mt := newMemTable(1024)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	for _, c := range chunks {
		assert.Equal(mt.addChunk(computeAddr(c), c), chunkAdded)
	}

	td1, _, err := buildTable(chunks[1:2])
	require.NoError(t, err)
	ti1, err := parseTableIndexByCopy(ctx, td1, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	tr1, err := newTableReader(ti1, tableReaderAtFromBytes(td1), fileBlockSize)
	require.NoError(t, err)
	defer tr1.close()
	assert.True(tr1.has(computeAddr(chunks[1]), nil))

	td2, _, err := buildTable(chunks[2:])
	require.NoError(t, err)
	ti2, err := parseTableIndexByCopy(ctx, td2, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	tr2, err := newTableReader(ti2, tableReaderAtFromBytes(td2), fileBlockSize)
	require.NoError(t, err)
	defer tr2.close()
	assert.True(tr2.has(computeAddr(chunks[2]), nil))

	_, data, count, err := mt.write(chunkReaderGroup{tr1, tr2}, &Stats{})
	require.NoError(t, err)
	assert.Equal(uint32(1), count)

	ti, err := parseTableIndexByCopy(ctx, data, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	outReader, err := newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
	require.NoError(t, err)
	defer outReader.close()
	assert.True(outReader.has(computeAddr(chunks[0]), nil))
	assert.False(outReader.has(computeAddr(chunks[1]), nil))
	assert.False(outReader.has(computeAddr(chunks[2]), nil))
}

type tableReaderAtAdapter struct {
	br *bytes.Reader
}

func tableReaderAtFromBytes(b []byte) tableReaderAt {
	return tableReaderAtAdapter{bytes.NewReader(b)}
}

func (adapter tableReaderAtAdapter) Close() error {
	return nil
}

func (adapter tableReaderAtAdapter) clone() (tableReaderAt, error) {
	return adapter, nil
}

func (adapter tableReaderAtAdapter) Reader(ctx context.Context) (io.ReadCloser, error) {
	r := *adapter.br
	return io.NopCloser(&r), nil
}

func (adapter tableReaderAtAdapter) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	return adapter.br.ReadAt(p, off)
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
		assert.Equal(mt.addChunk(computeAddr(c), c), chunkAdded)
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

func (crg chunkReaderGroup) has(h hash.Hash, keeper keeperF) (bool, gcBehavior, error) {
	for _, haver := range crg {
		ok, gcb, err := haver.has(h, keeper)
		if err != nil {
			return false, gcb, err
		}
		if gcb != gcBehavior_Continue {
			return true, gcb, nil
		}
		if ok {
			return true, gcb, nil
		}
	}
	return false, gcBehavior_Continue, nil
}

func (crg chunkReaderGroup) get(ctx context.Context, h hash.Hash, keeper keeperF, stats *Stats) ([]byte, gcBehavior, error) {
	for _, haver := range crg {
		if data, gcb, err := haver.get(ctx, h, keeper, stats); err != nil {
			return nil, gcb, err
		} else if gcb != gcBehavior_Continue {
			return nil, gcb, nil
		} else if data != nil {
			return data, gcb, nil
		}
	}

	return nil, gcBehavior_Continue, nil
}

func (crg chunkReaderGroup) hasMany(addrs []hasRecord, keeper keeperF) (bool, gcBehavior, error) {
	for _, haver := range crg {
		remaining, gcb, err := haver.hasMany(addrs, keeper)
		if err != nil {
			return false, gcb, err
		}
		if gcb != gcBehavior_Continue {
			return false, gcb, nil
		}
		if !remaining {
			return false, gcb, nil
		}
	}
	return true, gcBehavior_Continue, nil
}

func (crg chunkReaderGroup) getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	for _, haver := range crg {
		remaining, gcb, err := haver.getMany(ctx, eg, reqs, found, keeper, stats)
		if err != nil {
			return true, gcb, err
		}
		if gcb != gcBehavior_Continue {
			return true, gcb, nil
		}
		if !remaining {
			return false, gcb, nil
		}
	}
	return true, gcBehavior_Continue, nil
}

func (crg chunkReaderGroup) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	for _, haver := range crg {
		remaining, gcb, err := haver.getManyCompressed(ctx, eg, reqs, found, keeper, stats)
		if err != nil {
			return true, gcb, err
		}
		if gcb != gcBehavior_Continue {
			return true, gcb, nil
		}
		if !remaining {
			return false, gcb, nil
		}
	}
	return true, gcBehavior_Continue, nil
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

func (crg chunkReaderGroup) close() error {
	var firstErr error
	for _, c := range crg {
		err := c.close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
