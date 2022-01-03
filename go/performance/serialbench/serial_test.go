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
	"github.com/stretchr/testify/require"

)

var testKeyTuples = [][]byte{
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

var testValueTuples = [][]byte{
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
	KeyTuplesSz = 40
	ValueTuplesSz = 70

	TupleArraySz = 100
	LeafNodeSz = 264
)

func TestSerialFormat(t *testing.T) {
	t.Run("leaf node flatbuffer", func(t *testing.T) {
	})
	t.Run("test data sanity check", func(t *testing.T) {
		require.Equal(t, KeyTuplesSz, tupleSize(testKeyTuples))
		require.Equal(t, ValueTuplesSz, tupleSize(testValueTuples))
	})
}

func serializeTuples(t *testing.T, b *fb.Builder, tt [][]byte) fb.UOffsetT {
	//tupleSz := tupleSize(tt)
	//start := b.Offset()
	//serial.TupleArrayStartTuplesVector(b, tupleSz)
	//for i := len(tt) - 1; i >= 0; i-- {
	//	for j := len(tt[i]) - 1; j >= 0; j-- {
	//		b.PrependByte(tt[i][j])
	//	}
	//}
	//tuplesEnd := b.EndVector(tupleSz)
	//assert.Equal(t, tupleSz + 4, int(tuplesEnd - start))
	//
	//offsetSz := len(tt)
	//start = b.Offset()
	//serial.TupleArrayStartOffsetsVector(b, len(tt))
	//for i := len(tt) - 1; i >= 0; i-- {
	//	b.PrependUint16(uint16(len(tt[i])))
	//}
	//offsetsEnd := b.EndVector(offsetSz)
	//assert.Equal(t, (offsetSz * 2) + 4, int(offsetsEnd - start))
	//
	//start = b.Offset()
	//serial.TupleArrayStart(b)
	//assert.Equal(t, start, b.Offset())
	//serial.TupleArrayAddTuples(b, tuplesEnd)
	//assert.Equal(t, start + 4, b.Offset())
	//serial.TupleArrayAddOffsets(b, offsetsEnd)
	//assert.Equal(t, start + 8, b.Offset())
	//serial.TupleArrayAddFormat(b, serial.TupleTypeV1)
	//assert.Equal(t, start + 10, b.Offset())
	//
	//start = b.Offset()
	//arrEnd := serial.TupleArrayEnd(b)
	//off := b.Offset()
	//assert.Equal(t, arrEnd, b.Offset())
	//assert.Equal(t, start + 6, off)
	//
	//return arrEnd
	return 0
}

func tupleSize(tt [][]byte) (sz int) {
	for i := range tt {
		sz += len(tt[i])
	}
	return
}
