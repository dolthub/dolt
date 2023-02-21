// Copyright 2023 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestRoundTripIndexRecords(t *testing.T) {
	t.Run("table index record", func(t *testing.T) {
		start := uint64(0)
		for i := 0; i < 64; i++ {
			end := start + (rand.Uint64() % 1024)
			rec, buf := makeTableIndexRecord(start, end)
			start = end
			assert.Equal(t, rec.length, uint32(len(buf)))
			b := make([]byte, rec.length)
			n := writeJournalIndexRecord(b, rec.lastRoot, rec.start, rec.end, mustPayload(rec))
			assert.Equal(t, n, rec.length)
			assert.Equal(t, buf, b)
			r, err := readJournalIndexRecord(buf)
			assert.NoError(t, err)
			assert.Equal(t, rec, r)
		}
	})
}

func TestUnknownIndexRecordTag(t *testing.T) {
	// test behavior encountering unknown tag
	buf := makeUnknownTagIndexRecord()
	// checksum is ok
	ok := validateIndexRecord(buf)
	assert.True(t, ok)
	// reading record fails
	_, err := readJournalIndexRecord(buf)
	assert.Error(t, err)
}

func TestProcessIndexRecords(t *testing.T) {
	const cnt = 1024
	ctx := context.Background()
	records := make([]indexRec, cnt)
	buffers := make([][]byte, cnt)
	index := make([]byte, cnt*1024)

	var off uint32
	var start uint64
	for i := range records {
		end := start + (rand.Uint64() % 1024)
		r, b := makeTableIndexRecord(start, end)
		start = end
		off += writeJournalIndexRecord(index[off:], r.lastRoot, r.start, r.end, mustPayload(r))
		records[i], buffers[i] = r, b
	}
	index = index[:off]

	var i, sum int
	check := func(o int64, r indexRec) (_ error) {
		require.True(t, i < cnt)
		assert.Equal(t, records[i], r)
		assert.Equal(t, sum, int(o))
		sum += len(buffers[i])
		i++
		return
	}

	err := processIndexRecords(ctx, bytes.NewReader(index), int64(len(index)), check)
	assert.Equal(t, cnt, i)
	require.NoError(t, err)

	i, sum = 0, 0
	// write a bogus record to the end and process again
	index = appendCorruptIndexRecord(index)
	err = processIndexRecords(ctx, bytes.NewReader(index), int64(len(index)), check)
	assert.Equal(t, cnt, i)
	assert.Error(t, err) // fails to checksum
}

func TestRoundTripLookups(t *testing.T) {
	exp := makeLookups(128)
	buf := serializeLookups(exp)
	act := deserializeLookups(buf)
	assert.Equal(t, exp, act)

}

func makeTableIndexRecord(start, end uint64) (indexRec, []byte) {
	payload := randBuf(100)
	sz := journalIndexRecordSize(payload)
	lastRoot := hash.Of([]byte("fake commit"))

	var n int
	buf := make([]byte, sz)

	// length
	writeUint32(buf[n:], uint32(len(buf)))
	n += indexRecLenSz

	// last root
	buf[n] = byte(lastRootIndexRecTag)
	n += indexRecTagSz
	copy(buf[n:], lastRoot[:])
	n += len(lastRoot[:])

	// start offset
	buf[n] = byte(startOffsetIndexRecTag)
	n += indexRecTagSz
	writeUint64(buf[n:], start)
	n += indexRecOffsetSz

	// stop offset
	buf[n] = byte(endOffsetIndexRecTag)
	n += indexRecTagSz
	writeUint64(buf[n:], end)
	n += indexRecOffsetSz

	// kind
	buf[n] = byte(kindIndexRecTag)
	n += indexRecTagSz
	buf[n] = byte(tableIndexRecKind)
	n += indexRecKindSz

	// payload
	buf[n] = byte(payloadIndexRecTag)
	n += indexRecTagSz
	copy(buf[n:], payload)
	n += len(payload)

	// checksum
	c := crc(buf[:len(buf)-indexRecChecksumSz])
	writeUint32(buf[len(buf)-indexRecChecksumSz:], c)

	r := indexRec{
		length:   uint32(len(buf)),
		lastRoot: lastRoot,
		start:    start,
		end:      end,
		kind:     tableIndexRecKind,
		payload:  payload,
		checksum: c,
	}
	return r, buf
}

func makeUnknownTagIndexRecord() (buf []byte) {
	const fakeTag indexRecTag = 111
	_, buf = makeTableIndexRecord(0, 128)
	// overwrite recKind
	buf[indexRecLenSz] = byte(fakeTag)
	// redo checksum
	c := crc(buf[:len(buf)-indexRecChecksumSz])
	writeUint32(buf[len(buf)-indexRecChecksumSz:], c)
	return
}

func appendCorruptIndexRecord(buf []byte) []byte {
	tail := make([]byte, journalIndexRecordSize(nil))
	rand.Read(tail)
	// write a valid size, kind
	writeUint32(tail, uint32(len(tail)))
	tail[journalRecLenSz] = byte(tableIndexRecKind)
	return append(buf, tail...)
}

func mustPayload(rec indexRec) []byte {
	d.PanicIfFalse(rec.kind == tableIndexRecKind)
	return rec.payload
}

func makeLookups(cnt int) (lookups []lookup) {
	lookups = make([]lookup, cnt)
	buf := make([]byte, cnt*addrSize)
	rand.Read(buf)
	var off uint64
	for i := range lookups {
		copy(lookups[i].a[:], buf)
		buf = buf[addrSize:]
		lookups[i].r.Offset = off
		l := rand.Uint32() % 1024
		lookups[i].r.Length = l
		off += uint64(l)
	}
	return
}
