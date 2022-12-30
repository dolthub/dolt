// Copyright 2022 Dolthub, Inc.
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

package nbs

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func makeTestChunkJournal(t *testing.T) *chunkJournal {
	cacheOnce.Do(makeGlobalCaches)
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	m, err := getFileManifest(ctx, dir, syncFlush)
	require.NoError(t, err)
	q := NewUnlimitedMemQuotaProvider()
	p := newFSTablePersister(dir, globalFDCache, q)
	nbf := types.Format_Default.VersionString()
	j, err := newChunkJournal(ctx, nbf, dir, m, p.(*fsTablePersister))
	require.NoError(t, err)
	return j
}

func TestChunkJournalBlockStoreSuite(t *testing.T) {
	cacheOnce.Do(makeGlobalCaches)
	fn := func(ctx context.Context, dir string) (*NomsBlockStore, error) {
		q := NewUnlimitedMemQuotaProvider()
		nbf := types.Format_Default.VersionString()
		return NewLocalJournalingStore(ctx, nbf, dir, q)
	}
	suite.Run(t, &BlockStoreSuite{
		factory:        fn,
		skipInterloper: true,
	})
}

func TestChunkJournalPersist(t *testing.T) {
	ctx := context.Background()
	j := makeTestChunkJournal(t)
	const iters = 64
	stats := &Stats{}
	haver := emptyChunkSource{}
	for i := 0; i < iters; i++ {
		memTbl, chunkMap := randomMemTable(16)
		source, err := j.Persist(ctx, memTbl, haver, stats)
		assert.NoError(t, err)

		for h, ch := range chunkMap {
			ok, err := source.has(h)
			assert.NoError(t, err)
			assert.True(t, ok)
			data, err := source.get(ctx, h, stats)
			assert.NoError(t, err)
			assert.Equal(t, ch.Data(), data)
		}

		cs, err := j.Open(ctx, source.hash(), 16, stats)
		assert.NotNil(t, cs)
		assert.NoError(t, err)
	}
}

func TestReadRecordRanges(t *testing.T) {
	ctx := context.Background()
	j := makeTestChunkJournal(t)

	var buf []byte
	mt, data := randomMemTable(256)
	gets := make([]getRecord, 0, len(data))
	for h := range data {
		gets = append(gets, getRecord{a: &h, prefix: h.Prefix()})
	}

	jcs, err := j.Persist(ctx, mt, emptyChunkSource{}, &Stats{})
	require.NoError(t, err)

	rdr, sz, err := jcs.(journalChunkSource).journal.Snapshot()
	require.NoError(t, err)

	buf = make([]byte, sz)
	n, err := rdr.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, int(sz), n)

	ranges, err := jcs.getRecordRanges(gets)
	require.NoError(t, err)

	for h, rng := range ranges {
		b, err := jcs.get(ctx, addr(h), &Stats{})
		assert.NoError(t, err)
		ch1 := chunks.NewChunkWithHash(h, b)
		assert.Equal(t, data[addr(h)], ch1)

		start, stop := rng.Offset, uint32(rng.Offset)+rng.Length
		cc2, err := NewCompressedChunk(h, buf[start:stop])
		assert.NoError(t, err)
		ch2, err := cc2.ToChunk()
		assert.NoError(t, err)
		assert.Equal(t, data[addr(h)], ch2)
	}
}

func TestRoundTripRecords(t *testing.T) {
	t.Run("chunk record", func(t *testing.T) {
		for i := 0; i < 64; i++ {
			rec, buf := makeChunkRecord()
			assert.Equal(t, rec.length, uint32(len(buf)))
			b := make([]byte, rec.length)
			n := writeChunkRecord(b, mustCompressedChunk(rec))
			assert.Equal(t, n, rec.length)
			assert.Equal(t, buf, b)
			r := readJournalRecord(buf)
			assert.Equal(t, rec, r)
		}
	})
	t.Run("root hash record", func(t *testing.T) {
		for i := 0; i < 64; i++ {
			rec, buf := makeRootHashRecord()
			assert.Equal(t, rec.length, uint32(len(buf)))
			b := make([]byte, rec.length)
			n := writeRootHashRecord(b, rec.address)
			assert.Equal(t, n, rec.length)
			assert.Equal(t, buf, b)
			r := readJournalRecord(buf)
			assert.Equal(t, rec, r)
		}
	})
}

func TestProcessRecords(t *testing.T) {
	const cnt = 1024
	ctx := context.Background()
	records := make([]jrecord, cnt)
	buffers := make([][]byte, cnt)
	journal := make([]byte, cnt*1024)

	var off uint32
	for i := range records {
		var r jrecord
		var b []byte
		if i%8 == 0 {
			r, b = makeRootHashRecord()
			off += writeRootHashRecord(journal[off:], r.address)
		} else {
			r, b = makeChunkRecord()
			off += writeChunkRecord(journal[off:], mustCompressedChunk(r))
		}
		records[i], buffers[i] = r, b
	}

	var i, sum int
	check := func(o int64, r jrecord) (_ error) {
		require.True(t, i < cnt)
		assert.Equal(t, records[i], r)
		assert.Equal(t, sum, int(o))
		sum += len(buffers[i])
		i++
		return
	}

	n, err := processRecords(ctx, bytes.NewReader(journal), check)
	assert.Equal(t, cnt, i)
	assert.Equal(t, int(off), int(n))
	require.NoError(t, err)

	i, sum = 0, 0
	// write a bogus record to the end and process again
	writeCorruptRecord(journal[off:])
	n, err = processRecords(ctx, bytes.NewReader(journal), check)
	assert.Equal(t, cnt, i)
	assert.Equal(t, int(off), int(n))
	require.NoError(t, err)
}

func randomMemTable(cnt int) (*memTable, map[addr]chunks.Chunk) {
	chnx := make(map[addr]chunks.Chunk, cnt)
	for i := 0; i < cnt; i++ {
		ch := chunks.NewChunk(randBuf(100))
		chnx[addr(ch.Hash())] = ch
	}
	mt := newMemTable(uint64(cnt) * 256)
	for a, ch := range chnx {
		mt.addChunk(a, ch.Data())
	}
	return mt, chnx
}

func makeChunkRecord() (jrecord, []byte) {
	ch := chunks.NewChunk(randBuf(100))
	cc := ChunkToCompressedChunk(ch)
	payload := cc.FullCompressedChunk

	b := make([]byte, recMinSz+len(payload))
	writeUint(b, uint32(len(b)))
	b[recLenSz] = byte(chunkKind)
	copy(b[recLenSz+recKindSz:], cc.H[:])
	copy(b[recLenSz+recKindSz+addrSize:], payload)
	c := crc(b[:len(b)-checksumSize])
	writeUint(b[len(b)-checksumSize:], c)
	r := jrecord{
		length:   uint32(len(b)),
		kind:     chunkKind,
		address:  addr(cc.H),
		payload:  payload,
		checksum: c,
	}
	return r, b
}

func makeRootHashRecord() (jrecord, []byte) {
	a := addr(hash.Of(randBuf(8)))
	b := make([]byte, recMinSz)
	writeUint(b, uint32(len(b)))
	b[recLenSz] = byte(rootHashKind)
	copy(b[recLenSz+recKindSz:], a[:])
	c := crc(b[:len(b)-checksumSize])
	writeUint(b[len(b)-checksumSize:], c)
	r := jrecord{
		length:   uint32(len(b)),
		kind:     rootHashKind,
		payload:  b[len(b):],
		address:  a,
		checksum: c,
	}
	return r, b
}

func writeCorruptRecord(buf []byte) (n uint32) {
	// fill with random data
	rand.Read(buf[:recMinSz])
	// write a valid size, kind
	writeUint(buf, recMinSz)
	buf[recLenSz] = byte(rootHashKind)
	return recMinSz
}

func mustCompressedChunk(rec jrecord) CompressedChunk {
	d.PanicIfFalse(rec.kind == chunkKind)
	cc, err := NewCompressedChunk(hash.Hash(rec.address), rec.payload)
	d.PanicIfError(err)
	return cc
}

func randBuf(n int) (b []byte) {
	b = make([]byte, n)
	rand.Read(b)
	return
}
