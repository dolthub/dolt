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

import "encoding/binary"

const (
	countSize byteSize = 2
)

type Tuple []byte

func NewTuple(pool BuffPool, values ...Value) Tuple {
	pos := byteSize(0)
	for _, v := range values {
		if v.Null() {
			continue
		}
		pos += v.size()
	}

	tup, offs, mask := makeTuple(pool, pos, len(values))

	pos = byteSize(0)
	for i, v := range values {
		if v.Null() {
			continue
		}

		mask.set(i)
		offs.put(i, offset(pos))

		copy(tup[pos:pos+v.size()], v.Val)
		pos += v.size()
	}

	return nil
}

func makeTuple(pool BuffPool, bufSz byteSize, count int) (Tuple, offsetSlice, memberSet) {
	offSz := offsetSize(count)
	maskSz := maskSize(count)
	countSz := byteSize(2)

	tup := pool.Get(uint64(bufSz + offSz + maskSz + countSz))
	return tup, tup[bufSz : bufSz+offSz], tup[bufSz+offSz:]
}

func (tup Tuple) GetField(i int) []byte {
	offs, end := tup.offsetSlice()
	start := offs.get(i)
	if i != tup.Count() {
		end = offs.get(i+1)
	}
	return tup[start:end]
}

func (tup Tuple) Count() int {
	bb := tup[len(tup)-int(countSize):]
	return int(binary.LittleEndian.Uint16(bb))
}

func (tup Tuple) size() byteSize {
	return byteSize(len(tup))
}

func (tup Tuple) mask() memberSet {
	end := tup.size() - countSize
	start := end - maskSize(tup.Count())
	return memberSet(tup[start:end])
}

func (tup Tuple) offsetSlice() (sl offsetSlice, start offset) {
	mask := tup.mask()
	end := tup.size() - countSize - mask.size()
	start = offset(end - offsetSize(mask.count()))
	sl = offsetSlice(tup[start:end])
	return
}

type TupleDesc struct {
	types []Type
}

func NewTupleDescriptor(types ...Type) TupleDesc {
	return TupleDesc{types: types}
}

func (td TupleDesc) count() int {
	return len(td.types)
}

type offset uint16

type offsetSlice []byte

func makeOffsetSlice(pool BuffPool, count int) offsetSlice {
	return pool.Get(uint64(offsetSize(count)))
}

func offsetSize(count int) byteSize {
	return byteSize((count-1) * 2)
}

func (sl offsetSlice) count() int {
	return (len(sl) / 2) + 1
}

func (sl offsetSlice) get(i int) offset {
	if i == 0 {
		return 0
	}
	off := binary.LittleEndian.Uint16(sl[i*2 : (i*2)+2])
	return offset(off)
}

func (sl offsetSlice) put(i int, off offset) {
	if i == 0 {
		return
	}
	binary.LittleEndian.PutUint16(sl[i*2:(i*2)+2], uint16(off))
}
