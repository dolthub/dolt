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
		0x67, 0x0, 0x19, 0xd7, 0xf,
		0xb2, 0xe9, 0x5f, 0x5e, 0xaf,
		0xbb, 0x1e, 0x8, 0xed, 0x3,
		0x29, 0xd3, 0xf4, 0x1a, 0xb8,
	}
	LargeLeafNodeHash = hash.Hash{
		0x2e, 0x70, 0x35, 0x93, 0x1b,
		0xe7, 0x26, 0x80, 0x15, 0xee,
		0x2b, 0x31, 0xfe, 0x9a, 0x41,
		0x89, 0x6a, 0x2e, 0x5e, 0x9b,
	}
)

const (
	SmallLeafNodeSz    = 232
	SmallKeyTuplesSz   = 40
	SmallValueTuplesSz = 70

	// data       3200 = 800 + 3200
	// offsets    800  = sizeof(uint16) * (200 + 200)
	// metadata   15   = TupleFormat*2, tree_count, node_count, tree_level
	// flatbuffer 65     (1.6% overhead)
	// total size 4080
	LargeLeafNodeSz    = 4080
	LargeLeafNodeCount = 200
	LargeKeyTuplesSz   = 800
	LargeValueTuplesSz = 2400
)

func TestSerialFormat(t *testing.T) {
	t.Run("small leaf node", func(t *testing.T) {
		keys, values := smallTestTuples()
		buf := makeLeafNode(t, keys, values)
		assert.Equal(t, SmallLeafNodeSz, len(buf))
		assert.Equal(t, SmallLeafNodeHash, hash.Of(buf))
		validateLeafNode(t, buf, keys, values)
	})
	t.Run("large leaf node", func(t *testing.T) {
		keys, values := largeTestTuples()
		buf := makeLeafNode(t, keys, values)
		assert.Equal(t, LargeLeafNodeSz, len(buf))
		assert.Equal(t, LargeLeafNodeHash, hash.Of(buf))
		validateLeafNode(t, buf, keys, values)
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
	serial.MapStartKeyTuplesVector(b, keySz)
	start = int(b.Offset())
	keyTuples := serializeTuples(t, b, keys)
	assert.Equal(t, keyTuples, b.Offset())
	assert.Equal(t, start+keySz+4, int(b.Offset()))

	start = int(b.Offset())
	serial.MapStartKeyOffsetsVector(b, len(keys))
	keyOffsets := serializeOffsets(t, b, keys)
	assert.Equal(t, keyOffsets, b.Offset())
	assert.Equal(t, start+(2*len(keys))+4, int(b.Offset()))

	valSz := byteSize(values)
	serial.MapStartKeyTuplesVector(b, valSz)
	start = int(b.Offset())
	valTuples := serializeTuples(t, b, values)
	assert.Equal(t, valTuples, b.Offset())
	assert.Equal(t, start+valSz+4, int(b.Offset()))

	serial.MapStartKeyOffsetsVector(b, len(values))
	start = int(b.Offset())
	valOffsets := serializeOffsets(t, b, values)
	assert.Equal(t, valOffsets, b.Offset())
	assert.Equal(t, start+(2*len(values))+4, int(b.Offset()))

	start = int(b.Offset())
	serial.MapStart(b)
	assert.Equal(t, start, int(b.Offset()))
	serial.MapAddKeyTuples(b, keyTuples)
	assert.Equal(t, start+4, int(b.Offset()))
	serial.MapAddKeyOffsets(b, keyOffsets)
	assert.Equal(t, start+8, int(b.Offset()))
	serial.MapAddValueTuples(b, valTuples)
	assert.Equal(t, start+12, int(b.Offset()))
	serial.MapAddValueOffsets(b, valOffsets)
	assert.Equal(t, start+16, int(b.Offset()))
	serial.MapAddTreeCount(b, uint64(len(keys)))
	assert.Equal(t, start+24, int(b.Offset()))
	serial.MapAddNodeCount(b, uint16(len(keys)))
	assert.Equal(t, start+26, int(b.Offset()))
	serial.MapAddKeyFormat(b, serial.TupleFormatV1)
	assert.Equal(t, start+28, int(b.Offset()))
	serial.MapAddValueFormat(b, serial.TupleFormatV1)
	assert.Equal(t, start+30, int(b.Offset()))
	serial.MapAddTreeLevel(b, 0)
	assert.Equal(t, start+30, int(b.Offset()))

	mapEnd := serial.MapEnd(b)
	assert.Equal(t, start+58, int(b.Offset()))
	b.Finish(mapEnd)
	assert.Equal(t, start+64, int(b.Offset()))

	return b.FinishedBytes()
}

func validateLeafNode(t *testing.T, flatbuffer []byte, keys, values [][]byte) {
	require.Equal(t, len(keys), len(values))

	m := serial.GetRootAsMap(flatbuffer, 0)
	ko := make([]uint16, m.KeyOffsetsLength())
	vo := make([]uint16, m.ValueOffsetsLength())
	for i := range ko {
		ko[i] = m.KeyOffsets(i)
		vo[i] = m.ValueOffsets(i)
	}

	validateTuples(t, m.KeyTuplesBytes(), ko, keys)
	validateTuples(t, m.ValueTuplesBytes(), vo, values)

	assert.Equal(t, serial.TupleFormatV1, m.KeyFormat())
	assert.Equal(t, serial.TupleFormatV1, m.ValueFormat())
	assert.Equal(t, len(keys), int(m.TreeCount()))
	assert.Equal(t, len(keys), int(m.NodeCount()))
	assert.Equal(t, 0, int(m.TreeLevel()))
}

func validateTuples(t *testing.T, buf []byte, offs []uint16, tups [][]byte) {
	require.Equal(t, len(tups), len(offs))
	for i, exp := range tups {
		start, stop := offs[i], uint16(len(buf))
		if i+1 < len(offs) {
			stop = offs[i+1]
		}
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
	for i := len(tt) - 1; i >= 0; i-- {
		off -= len(tt[i])
		b.PrependUint16(uint16(off))
	}
	require.Equal(t, 0, off)
	return b.EndVector(len(tt))
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
