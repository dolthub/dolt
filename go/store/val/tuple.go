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

package val

import (
	"encoding/binary"

	"github.com/dolthub/dolt/go/store/pool"
)

const (
	numFieldsSize ByteSize = 2
)

type Tuple []byte

func TupleFromValues(pool pool.BuffPool, values ...Value) Tuple {
	var vv [][]byte // stack alloc
	for _, val := range values {
		vv = append(vv, val.Val)
	}
	return NewTuple(pool, vv...)
}

func NewTuple(pool pool.BuffPool, values ...[]byte) Tuple {
	count := 0
	pos := ByteSize(0)
	for _, v := range values {
		if isNull(v) {
			continue
		}
		count++
		pos += sizeOf(v)
	}

	tup, offs, mask := makeTuple(pool, pos, count, len(values))

	count = 0
	pos = ByteSize(0)
	for i, v := range values {
		if isNull(v) {
			continue
		}
		mask.set(i)
		offs.Put(count, pos)
		count++

		copy(tup[pos:pos+sizeOf(v)], v)
		pos += sizeOf(v)
	}

	return tup
}

func makeTuple(pool pool.BuffPool, bufSz ByteSize, values, fields int) (tup Tuple, offs Offsets, ms memberSet) {
	offSz := OffsetsSize(values)
	maskSz := maskSize(fields)
	countSz := numFieldsSize

	tup = pool.Get(uint64(bufSz + offSz + maskSz + countSz))

	writeFieldCount(tup, fields)
	offs = Offsets(tup[bufSz : bufSz+offSz])
	ms = memberSet(tup[bufSz+offSz : bufSz+offSz+maskSz])

	return
}

func (tup Tuple) GetField(i int) []byte {
	if i < 0 {
		// supports negative indexing
		i = tup.fieldCount() - i
	}

	if !tup.mask().present(i) {
		return nil // NULL
	}

	offs, bufStop := tup.offsets()
	i = tup.fieldToValue(i)

	start := offs.Get(i)

	var stop ByteSize
	if offs.IsLastIndex(i) {
		stop = bufStop
	} else {
		stop = offs.Get(i + 1)
	}

	return tup[start:stop]
}

func (tup Tuple) Count() int {
	return tup.fieldCount()
}

func (tup Tuple) size() ByteSize {
	return ByteSize(len(tup))
}

func (tup Tuple) fieldCount() int {
	sl := tup[tup.size()-numFieldsSize:]
	return int(binary.LittleEndian.Uint16(sl))
}

func (tup Tuple) valueCount() int {
	return tup.mask().count()
}

func (tup Tuple) mask() memberSet {
	stop := tup.size() - numFieldsSize
	start := stop - maskSize(tup.fieldCount())
	return memberSet(tup[start:stop])
}

func (tup Tuple) fieldToValue(i int) int {
	return tup.mask().countPrefix(i) - 1
}

func (tup Tuple) offsets() (offs Offsets, fieldStop ByteSize) {
	mask := tup.mask()
	offStop := tup.size() - numFieldsSize - mask.size()
	fieldStop = offStop - OffsetsSize(mask.count())
	offs = Offsets(tup[fieldStop:offStop])
	return
}

func isNull(val []byte) bool {
	return val == nil
}

func sizeOf(val []byte) ByteSize {
	return ByteSize(len(val))
}

func writeFieldCount(tup Tuple, count int) {
	binary.LittleEndian.PutUint16(tup[len(tup)-int(numFieldsSize):], uint16(count))
}
