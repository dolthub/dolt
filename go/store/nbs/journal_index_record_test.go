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
		for i := 0; i < 64; i++ {
			rec, buf := makeTableIndexRecord()
			assert.Equal(t, rec.length, uint32(len(buf)))
			b := make([]byte, rec.length)
			n := writeTableIndexRecord(b, rec.lastRoot, rec.start, rec.stop, mustPayload(rec))
			assert.Equal(t, n, rec.length)
			assert.Equal(t, buf, b)
			r, err := readTableIndexRecord(buf)
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
	_, err := readTableIndexRecord(buf)
	assert.Error(t, err)
}

func TestProcessIndexRecords(t *testing.T) {
	const cnt = 1024
	ctx := context.Background()
	records := make([]indexRec, cnt)
	buffers := make([][]byte, cnt)
	index := make([]byte, cnt*1024)

	var off uint32
	for i := range records {
		r, b := makeTableIndexRecord()
		off += writeTableIndexRecord(index[off:], r.lastRoot, r.start, r.stop, mustPayload(r))
		records[i], buffers[i] = r, b
	}

	var i, sum int
	check := func(o int64, r indexRec) (_ error) {
		require.True(t, i < cnt)
		assert.Equal(t, records[i], r)
		assert.Equal(t, sum, int(o))
		sum += len(buffers[i])
		i++
		return
	}

	n, err := processIndexRecords(ctx, bytes.NewReader(index), len(index), check)
	assert.Equal(t, cnt, i)
	assert.Equal(t, int(off), int(n))
	require.NoError(t, err)

	i, sum = 0, 0
	// write a bogus record to the end and process again
	writeCorruptIndexRecord(index[off:])
	n, err = processIndexRecords(ctx, bytes.NewReader(index), len(index), check)
	assert.Equal(t, cnt, i)
	assert.Equal(t, int(off), int(n))
	require.NoError(t, err)
}

func makeTableIndexRecord() (indexRec, []byte) {
	payload := randBuf(100)
	sz := tableIndexRecordSize(payload)
	lastRoot := hash.Of([]byte("fake commit"))
	start, stop := uint64(12345), uint64(23456)

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
	buf[n] = byte(stopOffsetIndexRecTag)
	n += indexRecTagSz
	writeUint64(buf[n:], stop)
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
		stop:     stop,
		kind:     tableIndexRecKind,
		payload:  payload,
		checksum: c,
	}
	return r, buf
}

func makeUnknownTagIndexRecord() (buf []byte) {
	const fakeTag indexRecTag = 111
	_, buf = makeTableIndexRecord()
	// overwrite recKind
	buf[indexRecLenSz] = byte(fakeTag)
	// redo checksum
	c := crc(buf[:len(buf)-indexRecChecksumSz])
	writeUint32(buf[len(buf)-indexRecChecksumSz:], c)
	return
}

func writeCorruptIndexRecord(buf []byte) (n uint32) {
	n = tableIndexRecordSize(nil)
	// fill with random data
	rand.Read(buf[:n])
	// write a valid size, kind
	writeUint32(buf, n)
	buf[journalRecLenSz] = byte(tableIndexRecKind)
	return
}

func mustPayload(rec indexRec) []byte {
	d.PanicIfFalse(rec.kind == tableIndexRecKind)
	return rec.payload
}
