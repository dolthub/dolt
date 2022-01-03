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
	"testing"

	fb "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
)

var testTuples = [][]byte{
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

const (
	TupleArraySz = 48
	TupleSliceSz = 40
)

func TestSerialFormat(t *testing.T) {
	var buf []byte

	buf = makeTuples(t)
	require.Equal(t, TupleArraySz, len(buf))
	require.Equal(t, TupleSliceSz, tupleSize(testTuples))
}

func makeTuples(t *testing.T) []byte {
	b := fb.NewBuilder(TupleArraySz)

	end := serializeTuples(b, testTuples)

	b.Finish(end)
	return b.FinishedBytes()
}

func serializeTuples(b *fb.Builder, tt [][]byte) fb.UOffsetT {
	sz := tupleSize(tt)
	serial.TupleArrayStartTuplesVector(b, sz)
	for i := len(tt) - 1; i >= 0; i-- {
		for j := len(tt[i]) - 1; j >= 0; j-- {
			b.PrependByte(tt[i][j])
		}
	}
	return b.EndVector(sz)
}

func tupleSize(tt [][]byte) (sz int) {
	for i := range tt {
		sz += len(tt[i])
	}
	return
}
