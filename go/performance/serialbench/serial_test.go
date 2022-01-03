// Copyright 2021 Dolthub, Inc.
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
	"testing"

	fb "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
)

var keys = [][]byte{
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

var values = [][]byte{
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

const (
	KeyTuplesSz   = 40
	ValueTuplesSz = 70

	LeafNodeSz = 232
)

var (
	LeafNodeHash = hash.Hash{
		0x8c, 0xfa, 0x4f, 0xf8, 0xc2,
		0x8c, 0xf7, 0x46, 0x72, 0x7e,
		0x5d, 0xef, 0x15, 0x16, 0xe6,
		0xec, 0xa7, 0x2d, 0x47, 0xe,
	}
)

func TestSerialFormat(t *testing.T) {
	t.Run("leaf node flatbuffer", func(t *testing.T) {
		buf := makeLeafNode(t, keys, values)
		assert.Equal(t, LeafNodeSz, len(buf))
		assert.Equal(t, LeafNodeHash, hash.Of(buf))
	})
	t.Run("test data sanity check", func(t *testing.T) {
		require.Equal(t, KeyTuplesSz, byteSize(keys))
		require.Equal(t, ValueTuplesSz, byteSize(values))
	})
}

func makeLeafNode(t *testing.T, keys, values [][]byte) []byte {
	b := fb.NewBuilder(LeafNodeSz)
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
	serial.MapAddTreeCount(b, 10)
	assert.Equal(t, start+24, int(b.Offset()))
	serial.MapAddNodeCount(b, 10)
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
