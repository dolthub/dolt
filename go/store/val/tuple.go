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
)

const (
	numFieldsSize byteSize = 2
)

type Tuple []byte

func NewTuple(pool BuffPool, values ...Value) Tuple {
	count := 0
	pos := byteSize(0)
	for _, v := range values {
		if v.Null() {
			continue
		}
		count++
		pos += v.size()
	}

	tup, offs, mask := makeTuple(pool, pos, count, len(values))

	count = 0
	pos = byteSize(0)
	for i, v := range values {
		if v.Null() {
			continue
		}
		mask.set(i)
		offs.put(count, offset(pos))
		count++

		copy(tup[pos:pos+v.size()], v.Val)
		pos += v.size()
	}

	return tup
}

func makeTuple(pool BuffPool, bufSz byteSize, values, fields int) (tup Tuple, offs offsetSlice, ms memberSet) {
	offSz := offsetSize(values)
	maskSz := maskSize(fields)
	countSz := numFieldsSize

	tup = pool.Get(uint64(bufSz + offSz + maskSz + countSz))

	writeFieldCount(tup, fields)
	offs = offsetSlice(tup[bufSz : bufSz+offSz])
	ms = memberSet(tup[bufSz+offSz : bufSz+offSz+maskSz])

	return
}

func writeFieldCount(tup Tuple, count int) {
	binary.LittleEndian.PutUint16(tup[len(tup)-int(numFieldsSize):], uint16(count))
}

func (tup Tuple) GetField(i int) []byte {
	if !tup.mask().present(i) {
		return nil
	}

	offs, bufStop := tup.offsetSlice()
	i = tup.fieldToValue(i)

	start := offs.get(i)

	var stop offset
	if offs.isLastIndex(i) {
		stop = bufStop
	} else {
		stop = offs.get(i + 1)
	}

	return tup[start:stop]
}

func (tup Tuple) size() byteSize {
	return byteSize(len(tup))
}

func (tup Tuple) fieldCount() int {
	bb := tup[len(tup)-int(numFieldsSize):]
	return int(binary.LittleEndian.Uint16(bb))
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

func (tup Tuple) offsetSlice() (offs offsetSlice, bufStop offset) {
	mask := tup.mask()
	offStop := tup.size() - numFieldsSize - mask.size()
	bufStop = offset(offStop - offsetSize(mask.count()))
	offs = offsetSlice(tup[bufStop:offStop])
	return
}

type offset uint16

type offsetSlice []byte

func offsetSize(count int) byteSize {
	if count == 0 {
		return 0
	}
	return byteSize((count - 1) * 2)
}

func (sl offsetSlice) count() int {
	return (len(sl) / 2) + 1
}

func (sl offsetSlice) get(i int) offset {
	if i == 0 {
		return 0
	}
	start := (i - 1) * 2
	off := binary.LittleEndian.Uint16(sl[start : start+2])
	return offset(off)
}

func (sl offsetSlice) put(i int, off offset) {
	if i == 0 {
		return
	}
	start := (i - 1) * 2
	binary.LittleEndian.PutUint16(sl[start:start+2], uint16(off))
}

func (sl offsetSlice) isLastIndex(i int) bool {
	return len(sl) == i*2
}
