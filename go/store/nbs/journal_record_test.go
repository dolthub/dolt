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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

func testTimestampGenerator() uint64 {
	return 42
}

func TestRoundTripJournalRecords(t *testing.T) {
	t.Run("chunk record", func(t *testing.T) {
		for i := 0; i < 64; i++ {
			rec, buf := makeChunkRecord()
			assert.Equal(t, rec.length, uint32(len(buf)))
			b := make([]byte, rec.length)
			n := writeChunkRecord(b, mustCompressedChunk(rec))
			assert.Equal(t, n, rec.length)
			assert.Equal(t, buf, b)
			r, err := readJournalRecord(buf)
			assert.NoError(t, err)
			assert.Equal(t, rec, r)
		}
	})

	// Root hash records contain a timestamp, so override the journal record timestamp
	// generator function with a test version that returns a known, predictable value.
	journalRecordTimestampGenerator = testTimestampGenerator

	t.Run("root hash record", func(t *testing.T) {
		for i := 0; i < 64; i++ {
			rec, buf := makeRootHashRecord()
			assert.Equal(t, rec.length, uint32(len(buf)))
			b := make([]byte, rec.length)
			n := writeRootHashRecord(b, rec.address)
			assert.Equal(t, n, rec.length)
			assert.Equal(t, buf, b)
			r, err := readJournalRecord(buf)
			assert.NoError(t, err)
			assert.Equal(t, rec, r)
		}
	})
}

func TestUnknownJournalRecordTag(t *testing.T) {
	// test behavior encountering unknown tag
	buf := makeUnknownTagJournalRecord()
	// checksum is ok
	err := validateJournalRecord(buf)
	assert.NoError(t, err)
	// reading record fails
	_, err = readJournalRecord(buf)
	assert.Error(t, err)
}

func TestProcessJournalRecords(t *testing.T) {
	const cnt = 1024
	ctx := context.Background()
	records := make([]journalRec, cnt)
	buffers := make([][]byte, cnt)
	journal := make([]byte, cnt*1024)

	// Root hash records contain a timestamp, so override the journal record timestamp
	// generator function with a test version that returns a known, predictable value.
	journalRecordTimestampGenerator = testTimestampGenerator

	var off uint32
	for i := range records {
		var r journalRec
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
	check := func(o int64, r journalRec) (_ error) {
		require.True(t, i < cnt)
		assert.Equal(t, records[i], r)
		assert.Equal(t, sum, int(o))
		sum += len(buffers[i])
		i++
		return
	}

	n, err := processJournalRecords(ctx, bytes.NewReader(journal), 0, check)
	assert.Equal(t, cnt, i)
	assert.Equal(t, int(off), int(n))
	require.NoError(t, err)

	// write a bogus record to the end and verify that we get an error
	i, sum = 0, 0
	writeCorruptJournalRecord(journal[off:])
	n, err = processJournalRecords(ctx, bytes.NewReader(journal), 0, check)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CRC checksum does not match")
	assert.Equal(t, cnt, i)
	// Since an error was encountered, the returned offset is 0
	assert.Equal(t, 0, int(n))

	// Turn on the env setting to stop processing journal records once we hit an invalid record
	require.NoError(t, os.Setenv(dconfig.EnvSkipInvalidJournalRecords, "1"))
	i, sum = 0, 0
	// write a bogus record to the end and process again
	writeCorruptJournalRecord(journal[off:])
	n, err = processJournalRecords(ctx, bytes.NewReader(journal), 0, check)
	require.NoError(t, err)
	assert.Equal(t, cnt, i)
	assert.Equal(t, int(off), int(n))
}

func randomMemTable(cnt int) (*memTable, map[hash.Hash]chunks.Chunk) {
	chnx := make(map[hash.Hash]chunks.Chunk, cnt)
	for i := 0; i < cnt; i++ {
		ch := chunks.NewChunk(randBuf(100))
		chnx[ch.Hash()] = ch
	}
	mt := newMemTable(uint64(cnt) * 256)
	for a, ch := range chnx {
		mt.addChunk(a, ch.Data())
	}
	return mt, chnx
}

func makeChunkRecord() (journalRec, []byte) {
	ch := chunks.NewChunk(randBuf(100))
	cc := ChunkToCompressedChunk(ch)
	payload := cc.FullCompressedChunk
	sz, _ := chunkRecordSize(cc)

	var n int
	buf := make([]byte, sz)
	// length
	writeUint32(buf[n:], uint32(len(buf)))
	n += journalRecLenSz
	// kind
	buf[n] = byte(kindJournalRecTag)
	n += journalRecTagSz
	buf[n] = byte(chunkJournalRecKind)
	n += journalRecKindSz
	// address
	buf[n] = byte(addrJournalRecTag)
	n += journalRecTagSz
	copy(buf[n:], cc.H[:])
	n += journalRecAddrSz
	// payload
	buf[n] = byte(payloadJournalRecTag)
	n += journalRecTagSz
	copy(buf[n:], payload)
	n += len(payload)
	// checksum
	c := crc(buf[:len(buf)-journalRecChecksumSz])
	writeUint32(buf[len(buf)-journalRecChecksumSz:], c)

	r := journalRec{
		length:   uint32(len(buf)),
		kind:     chunkJournalRecKind,
		address:  cc.H,
		payload:  payload,
		checksum: c,
	}
	return r, buf
}

func makeRootHashRecord() (journalRec, []byte) {
	a := hash.Of(randBuf(8))
	var n int
	buf := make([]byte, rootHashRecordSize())
	// length
	writeUint32(buf[n:], uint32(len(buf)))
	n += journalRecLenSz
	// kind
	buf[n] = byte(kindJournalRecTag)
	n += journalRecTagSz
	buf[n] = byte(rootHashJournalRecKind)
	n += journalRecKindSz
	// timestamp
	buf[n] = byte(timestampJournalRecTag)
	n += journalRecTagSz
	writeUint64(buf[n:], testTimestampGenerator())
	n += journalRecTimestampSz
	// address
	buf[n] = byte(addrJournalRecTag)
	n += journalRecTagSz
	copy(buf[n:], a[:])
	n += journalRecAddrSz
	// checksum
	c := crc(buf[:len(buf)-journalRecChecksumSz])
	writeUint32(buf[len(buf)-journalRecChecksumSz:], c)
	r := journalRec{
		length:    uint32(len(buf)),
		kind:      rootHashJournalRecKind,
		address:   a,
		checksum:  c,
		timestamp: time.Unix(int64(testTimestampGenerator()), 0),
	}
	return r, buf
}

func makeUnknownTagJournalRecord() (buf []byte) {
	const fakeTag journalRecTag = 111
	_, buf = makeRootHashRecord()
	// overwrite recKind
	buf[journalRecLenSz] = byte(fakeTag)
	// redo checksum
	c := crc(buf[:len(buf)-journalRecChecksumSz])
	writeUint32(buf[len(buf)-journalRecChecksumSz:], c)
	return
}

func writeCorruptJournalRecord(buf []byte) (n uint32) {
	n = uint32(rootHashRecordSize())
	// fill with random data
	rand.Read(buf[:n])
	// write a valid size, kind
	writeUint32(buf, n)
	buf[journalRecLenSz] = byte(rootHashJournalRecKind)
	return
}

func mustCompressedChunk(rec journalRec) CompressedChunk {
	d.PanicIfFalse(rec.kind == chunkJournalRecKind)
	cc, err := NewCompressedChunk(hash.Hash(rec.address), rec.payload)
	d.PanicIfError(err)
	return cc
}
