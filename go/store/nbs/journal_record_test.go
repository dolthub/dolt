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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	var recoverErr error
	n, err := processJournalRecords(ctx, bytes.NewReader(journal), 0, check, func(e error) { recoverErr = e })
	assert.Equal(t, cnt, i)
	assert.Equal(t, int(off), int(n))
	require.NoError(t, err)
	require.NoError(t, recoverErr)

	// write a bogus record to the end and verify that we don't get an error
	i, sum = 0, 0
	writeCorruptJournalRecord(journal[off:])
	n, err = processJournalRecords(ctx, bytes.NewReader(journal), 0, check, func(e error) { recoverErr = e })
	require.NoError(t, err)
	assert.Equal(t, cnt, i)
	assert.Equal(t, int(off), int(n))
	require.Error(t, recoverErr)
}

func TestJournalForDataLoss(t *testing.T) {
	type section byte
	const (
		root section = iota // root hash record
		chnk                // chunk record
		garb                // garbage data.
		null                // null bytes.
	)
	type journalDesc struct {
		lossExpected     bool
		recoveryExpected bool
		rootsExpected    int
		chunksExpected   int
		sections         []section
	}

	tests := []journalDesc{
		// Normal cases - records followed by 4 null bytes, EOF, or null bytes then garbage
		{false, false, 1, 0, []section{root}},
		{false, false, 1, 0, []section{root, null, null, null}},
		{false, false, 2, 1, []section{root, chnk, root, null, chnk, chnk}}, // No roots after the null bytes.
		{false, false, 1, 0, []section{root, null, root}},                   // a single root is not data loss - needs to be followed by a valid record.
		{false, false, 1, 0, []section{root, null, garb, null, root, null}},
		{false, false, 1, 0, []section{root, null, garb, null, root, garb}},

		// Recovery cases when non-null bytes immediately follow a record of any type.
		{false, true, 1, 0, []section{root, garb, garb, garb}},
		{false, true, 1, 2, []section{root, chnk, chnk, garb}}, // valid chunks still get reported to callback, even if they aren't followed by a root record.
		{false, true, 1, 0, []section{root, garb, null, chnk, chnk}},

		// Data loss cases. Any mystery data which has a sequence of a parsable root followed by any parsable records is data loss.
		{true, false, 1, 0, []section{root, null, root, chnk}},
		{true, false, 2, 1, []section{root, chnk, root, null, root, chnk, chnk, chnk}},
		{true, true, 1, 0, []section{root, garb, root, chnk}},
		{true, false, 1, 0, []section{root, null, root, root}},
		{true, true, 1, 0, []section{root, garb, root, root}},
		{true, false, 1, 0, []section{root, null, root, chnk, chnk, null}},
		{true, true, 1, 0, []section{root, garb, root, chnk, chnk, garb}},
		{true, false, 1, 0, []section{root, null, root, null, root, null, root, garb}},
		{true, false, 1, 0, []section{root, null, root, garb, root, garb, root, garb}},
		{true, true, 1, 0, []section{root, garb, root, null, root, null, root, null}},

		// Chunks in the suffix garbage shouldn't matter.
		{false, false, 1, 0, []section{root, null, chnk, chnk, chnk}},
		{false, false, 1, 0, []section{root, null, chnk, chnk, chnk, root}},
	}

	journalRecordTimestampGenerator = testTimestampGenerator

	rnd := rand.New(rand.NewSource(123454321))

	for ti, td := range tests {
		_ = td
		t.Run(fmt.Sprintf("data check %d", ti), func(t *testing.T) {
			ctx := context.Background()
			journal := make([]byte, 1<<20) // 1 MB should be plenty for these tests.

			var off uint32
			for _, section := range td.sections {
				var r journalRec
				switch section {
				case root:
					r, _ = makeRootHashRecord()
					off += writeRootHashRecord(journal[off:], r.address)
				case chnk:
					r, _ = makeChunkRecord()
					off += writeChunkRecord(journal[off:], mustCompressedChunk(r))
				case garb:
					n := uint32(rnd.Intn(256) + 256)
					rnd.Read(journal[off : off+n])
					off += n
				case null:
					n := uint32(rand.Intn(256) + 256)
					for i := uint32(0); i < n; i++ {
						journal[off+i] = 0
					}
					off += n
				}
			}

			// When we go into the recovery state, we should not call the call back for any more records.
			// Verify that here with counters of each record type.
			chunksFound := 0
			rootsFound := 0
			check := func(o int64, r journalRec) (_ error) {
				switch r.kind {
				case rootHashJournalRecKind:
					rootsFound++
				case chunkJournalRecKind:
					chunksFound++
				}
				return
			}

			var recoverErr error
			_, err := processJournalRecords(ctx, bytes.NewReader(journal[:off]), 0, check, func(e error) { recoverErr = e })

			if td.lossExpected {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, td.chunksExpected, chunksFound)
			require.Equal(t, td.rootsExpected, rootsFound)
			if td.recoveryExpected {
				require.Error(t, recoverErr)
			} else {
				require.NoError(t, recoverErr)
			}
		})
	}
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
