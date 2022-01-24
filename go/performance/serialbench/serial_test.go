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

package serialbench

import (
	"encoding/binary"
	"testing"

	fb "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
)

var (
	SmallLeafNodeHash = hash.Hash{
		0xb5, 0x72, 0x9e, 0x97, 0x52,
		0x3a, 0x8f, 0x75, 0x7f, 0x3b,
		0x2e, 0xf9, 0x2a, 0x5f, 0x13,
		0x7d, 0x9a, 0x1e, 0x73, 0x3d,
	}
	LargeLeafNodeHash = hash.Hash{
		0xba, 0xa5, 0xca, 0x73, 0x44,
		0x89, 0xf6, 0xd0, 0x9d, 0xf8,
		0x26, 0xee, 0xba, 0x40, 0x88,
		0x95, 0x74, 0x1e, 0x7e, 0xa1,
	}
)

const (
	SmallLeafNodeSz     = 224
	SmallKeyTuplesSz    = 40
	SmallValueTuplesSz  = 70
	SmallKeyOffsetsSz   = 18
	SmallValueOffsetsSz = 18

	// data        3200 = 800 + 3200
	// offsets     796  = sizeof(uint16) * (199 + 199)
	// metadata    11   = TupleFormat * 2, tree_count, tree_level
	// flatbuffers 65   = (1.6% overhead)
	// total size  4072
	LargeLeafNodeSz     = 4072
	LargeLeafNodeCount  = 200
	LargeKeyTuplesSz    = 800
	LargeValueTuplesSz  = 2400
	LargeKeyOffsetsSz   = 398
	LargeValueOffsetsSz = 398
)

func TestSerialFormat(t *testing.T) {
	t.Run("small leaf node", func(t *testing.T) {
		keys, values := smallTestTuples()
		buf := makeLeafNode(t, keys, values)
		assert.Equal(t, SmallLeafNodeSz, len(buf))
		assert.Equal(t, SmallLeafNodeHash, hash.Of(buf))

		m := serial.GetRootAsTupleMap(buf, 0)
		validateLeafNode(t, m, keys, values)
		assert.Equal(t, SmallKeyTuplesSz, len(m.KeyTuplesBytes()))
		assert.Equal(t, SmallValueTuplesSz, len(m.ValueTuplesBytes()))
		assert.Equal(t, SmallKeyOffsetsSz, m.KeyOffsetsLength()*2)
		assert.Equal(t, SmallValueOffsetsSz, m.ValueOffsetsLength()*2)
	})
	t.Run("large leaf node", func(t *testing.T) {
		keys, values := largeTestTuples()
		buf := makeLeafNode(t, keys, values)
		assert.Equal(t, LargeLeafNodeSz, len(buf))
		assert.Equal(t, LargeLeafNodeHash, hash.Of(buf))

		m := serial.GetRootAsTupleMap(buf, 0)
		validateLeafNode(t, m, keys, values)
		assert.Equal(t, LargeKeyTuplesSz, len(m.KeyTuplesBytes()))
		assert.Equal(t, LargeValueTuplesSz, len(m.ValueTuplesBytes()))
		assert.Equal(t, LargeKeyOffsetsSz, m.KeyOffsetsLength()*2)
		assert.Equal(t, LargeValueOffsetsSz, m.ValueOffsetsLength()*2)
	})
	t.Run("test data sanity check", func(t *testing.T) {
		keys, values := smallTestTuples()
		require.Equal(t, SmallKeyTuplesSz, byteSize(keys))
		require.Equal(t, SmallValueTuplesSz, byteSize(values))
		require.Equal(t, 10, len(keys))
		require.Equal(t, 10, len(values))

		keys, values = largeTestTuples()
		require.Equal(t, LargeKeyTuplesSz, byteSize(keys))
		require.Equal(t, LargeValueTuplesSz, byteSize(values))
		require.Equal(t, LargeLeafNodeCount, len(keys))
		require.Equal(t, LargeLeafNodeCount, len(values))
	})
}

func makeLeafNode(t *testing.T, keys, values [][]byte) []byte {
	b := fb.NewBuilder((byteSize(keys) + byteSize(values)) * 2)
	start := int(b.Offset())
	assert.Equal(t, 0, start)

	keySz := byteSize(keys)
	serial.TupleMapStartKeyTuplesVector(b, keySz)
	start = int(b.Offset())
	keyTuples := serializeTuples(t, b, keys)
	assert.Equal(t, keyTuples, b.Offset())
	assert.Equal(t, start+keySz+4, int(b.Offset()))

	start = int(b.Offset())
	// zeroth offset ommitted
	ol := len(keys) - 1
	serial.TupleMapStartKeyOffsetsVector(b, ol)
	keyOffsets := serializeOffsets(t, b, keys)
	assert.Equal(t, keyOffsets, b.Offset())
	offsetsSz := (2 * (len(keys) - 1)) + 4
	assert.Equal(t, padToMultiple(start+offsetsSz, 4), int(b.Offset()))

	valSz := byteSize(values)
	serial.TupleMapStartValueOffsetsVector(b, valSz)
	start = int(b.Offset())
	valTuples := serializeTuples(t, b, values)
	assert.Equal(t, valTuples, b.Offset())
	assert.Equal(t, start+valSz+4, int(b.Offset()))

	// zeroth offset ommitted
	serial.TupleMapStartValueOffsetsVector(b, len(values)-1)
	start = int(b.Offset())
	valOffsets := serializeOffsets(t, b, values)
	assert.Equal(t, valOffsets, b.Offset())
	offsetsSz = (2 * (len(values) - 1)) + 4
	assert.Equal(t, padToMultiple(start+offsetsSz, 4), int(b.Offset()))

	start = int(b.Offset())
	serial.TupleMapStart(b)
	assert.Equal(t, start, int(b.Offset()))

	serial.TupleMapAddTreeCount(b, uint64(len(keys)))
	// write map elements in descending order by size
	start = padToMultiple(start, 8)
	assert.Equal(t, start+8, int(b.Offset()))

	// each vector reference is a uint32
	serial.TupleMapAddKeyTuples(b, keyTuples)
	assert.Equal(t, start+12, int(b.Offset()))
	serial.TupleMapAddKeyOffsets(b, keyOffsets)
	assert.Equal(t, start+16, int(b.Offset()))
	serial.TupleMapAddValueTuples(b, valTuples)
	assert.Equal(t, start+20, int(b.Offset()))
	serial.TupleMapAddValueOffsets(b, valOffsets)
	assert.Equal(t, start+24, int(b.Offset()))

	serial.TupleMapAddKeyFormat(b, serial.TupleFormatV1)
	assert.Equal(t, start+25, int(b.Offset()))
	serial.TupleMapAddValueFormat(b, serial.TupleFormatV1)
	assert.Equal(t, start+26, int(b.Offset()))

	// default value of 0 ommitted
	serial.TupleMapAddTreeLevel(b, 0)
	assert.Equal(t, start+26, int(b.Offset()))

	mapEnd := serial.TupleMapEnd(b)
	assert.Equal(t, start+52, int(b.Offset()))
	b.Finish(mapEnd)
	assert.Equal(t, start+56, int(b.Offset()))

	return b.FinishedBytes()
}

func validateLeafNode(t *testing.T, m *serial.TupleMap, keys, values [][]byte) {
	require.Equal(t, len(keys), len(values))

	assert.Equal(t, len(keys)-1, m.KeyOffsetsLength())
	kb := make([]uint16, m.KeyOffsetsLength()+2)
	vb := make([]uint16, m.ValueOffsetsLength()+2)
	ktb := m.KeyTuplesBytes()
	vtb := m.ValueTuplesBytes()

	kb[0], vb[0] = 0, 0
	kb[len(kb)-1], vb[len(vb)-1] = uint16(len(ktb)), uint16(len(vtb))
	for i := 0; i < m.KeyOffsetsLength(); i++ {
		kb[i+1] = m.KeyOffsets(i)
		vb[i+1] = m.ValueOffsets(i)
	}

	validateTuples(t, ktb, kb, keys)
	validateTuples(t, vtb, vb, values)

	assert.Equal(t, serial.TupleFormatV1, m.KeyFormat())
	assert.Equal(t, serial.TupleFormatV1, m.ValueFormat())
	assert.Equal(t, len(keys), int(m.TreeCount()))
	assert.Equal(t, 0, int(m.TreeLevel()))
}

func validateTuples(t *testing.T, buf []byte, bounds []uint16, tups [][]byte) {
	for i, exp := range tups {
		start, stop := bounds[i], bounds[i+1]
		act := buf[start:stop]
		assert.Equal(t, act, exp)
	}
}

func serializeTuples(t *testing.T, b *fb.Builder, tt [][]byte) fb.UOffsetT {
	for i := len(tt) - 1; i >= 0; i-- {
		for j := len(tt[i]) - 1; j >= 0; j-- {
			b.PrependByte(tt[i][j])
		}
	}
	return b.EndVector(byteSize(tt))
}

func serializeOffsets(t *testing.T, b *fb.Builder, tt [][]byte) fb.UOffsetT {
	off := byteSize(tt)
	for i := len(tt) - 1; i > 0; i-- {
		off -= len(tt[i])
		b.PrependUint16(uint16(off))
	}
	// zeroth offset ommitted
	require.Equal(t, len(tt[0]), off)
	return b.EndVector(len(tt) - 1)
}

func byteSize(tt [][]byte) (sz int) {
	for i := range tt {
		sz += len(tt[i])
	}
	return
}

func smallTestTuples() (keys, values [][]byte) {
	keys = [][]byte{
		[]byte("zero"),
		[]byte("one"),
		[]byte("two"),
		[]byte("three"),
		[]byte("four"),
		[]byte("five"),
		[]byte("six"),
		[]byte("seven"),
		[]byte("eight"),
		[]byte("nine"),
	}
	values = [][]byte{
		[]byte("ten"),
		[]byte("eleven"),
		[]byte("twelve"),
		[]byte("thirteen"),
		[]byte("fourteen"),
		[]byte("fifteen"),
		[]byte("sixteen"),
		[]byte("seventeen"),
		[]byte("eighteen"),
		[]byte("nineteen"),
	}
	return
}

func largeTestTuples() (keys, values [][]byte) {
	keys = make([][]byte, LargeLeafNodeCount)
	values = make([][]byte, LargeLeafNodeCount)
	for i := range keys {
		keys[i] = make([]byte, 4)
		binary.LittleEndian.PutUint32(keys[i], uint32(i))
		values[i] = make([]byte, 12)
		binary.LittleEndian.PutUint32(values[i][0:4], uint32(i))
		binary.LittleEndian.PutUint32(values[i][4:8], uint32(i*2))
		binary.LittleEndian.PutUint32(values[i][8:12], uint32(i*3))
	}
	return
}

func padToMultiple(i, k int) int {
	for {
		if i%k == 0 {
			return i
		}
		i++
	}
}
