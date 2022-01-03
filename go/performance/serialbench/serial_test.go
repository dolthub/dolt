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
		0x8c, 0xfa, 0x4f, 0xf8, 0xc2,
		0x8c, 0xf7, 0x46, 0x72, 0x7e,
		0x5d, 0xef, 0x15, 0x16, 0xe6,
		0xec, 0xa7, 0x2d, 0x47, 0xe,
	}
	LargeLeafNodeHash = hash.Hash{
		0xfe, 0xea, 0xc9, 0xbc, 0x9f,
		0x3, 0x1d, 0xa7, 0xac, 0xfc,
		0xee, 0xf4, 0x82, 0xe8, 0xb6,
		0x8b, 0xe0, 0x5c, 0xbc, 0xaa,
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
	})
	t.Run("large leaf node", func(t *testing.T) {
		keys, values := largeTestTuples()
		buf := makeLeafNode(t, keys, values)
		assert.Equal(t, LargeLeafNodeSz, len(buf))
		assert.Equal(t, LargeLeafNodeHash, hash.Of(buf))
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
	serial.MapAddTreeLevel(b, 0)
	assert.Equal(t, start+26, int(b.Offset()))

	mapEnd := serial.MapEnd(b)
	assert.Equal(t, start+54, int(b.Offset()))
	b.Finish(mapEnd)
	assert.Equal(t, start+64, int(b.Offset()))

	return b.FinishedBytes()
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
