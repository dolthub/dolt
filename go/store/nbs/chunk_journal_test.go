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
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestChunkJournalAddr(t *testing.T) {
	expAddr := addr{
		255, 255, 255, 255, 255,
		255, 255, 255, 255, 255,
		255, 255, 255, 255, 255,
		255, 255, 255, 255, 255,
	}
	expString := "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
	assert.Equal(t, expAddr, chunkJournalAddr)
	assert.Equal(t, expString, chunkJournalAddr.String())
	assert.Equal(t, uint64(math.MaxUint64), chunkJournalAddr.Prefix())
	assert.Equal(t, uint32(math.MaxUint32), chunkJournalAddr.Checksum())
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

	n, c, err := processRecords(ctx, bytes.NewBuffer(journal), check)
	assert.Equal(t, cnt, int(c))
	assert.Equal(t, int(off), int(n))
	require.NoError(t, err)

	i, sum = 0, 0
	// write a bogus record to the end and process again
	writeCorruptRecord(journal[off:])
	n, c, err = processRecords(ctx, bytes.NewBuffer(journal), check)
	assert.Equal(t, cnt, int(c))
	assert.Equal(t, int(off), int(n))
	require.NoError(t, err)
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
