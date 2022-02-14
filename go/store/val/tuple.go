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
	"math"

	"github.com/dolthub/dolt/go/store/pool"
)

const (
	MaxTupleFields            = 4096
	MaxTupleDataSize ByteSize = math.MaxUint16

	numFieldsSize ByteSize = 2
)

// Tuples are byte slices containing field values and a footer. Tuples only
//   contain Values for non-NULL Fields. Value i contains the data for ith non-
//   NULL Field. Values are packed contiguously from the front of the Tuple. The
//   footer contains offsets, a member mask, and a field count. offsets enable
//   random access to Values. The member mask enables NULL-compaction for Values.
//
//   Tuples read and write Values as byte slices. (De)serialization is delegated
//   to Tuple Descriptors, which know a Tuple's schema and associated encodings.
//   When reading and writing Values, NULLs are encoded as nil byte slices. Note
//   that these are not the same as zero-length byte slices. An empty string may
//   be encoded as a zero-length byte slice and will be distinct from a NULL
//   string both logically and semantically.
//
//   Tuple:
//   +---------+---------+-----+---------+---------+-------------+-------------+
//   | Value 0 | Value 1 | ... | Value K | offsets | Member Mask | Field Count |
//   +---------+---------+-----+---------+---------+-------------+-------------+
//
//   offsets:
//     The offset array contains a uint16 for each non-NULL field after field 0.
//     Offset i encodes the distance to the ith Value from the front of the Tuple.
//     The size of the offset array is 2*(K-1) bytes, where K is the number of
//     Values in the Tuple.
//   +----------+----------+-----+----------+
//   | Offset 1 | Offset 2 | ... | Offset K |
//   +----------+----------+-----+----------+
//
//   Member Mask:
//     The member mask is a bit-array encoding field membership in Tuples. Fields
//     with non-NULL values are present, and encoded as 1, NULL fields are absent
//     and encoded as 0. The size of the bit array is math.Ceil(N/8) bytes, where
//     N is the number of Fields in the Tuple.
//   +------------+-------------+-----+
//   | Bits 0 - 7 | Bits 8 - 15 | ... |
//   +------------+-------------+-----+
//
//   Field Count:
//      The field fieldCount is a uint16 containing the number of fields in the
//     	Tuple, it is stored in 2 bytes.
//   +----------------------+
//   | Field Count (uint16) |
//   +----------------------+

type Tuple []byte

var EmptyTuple = Tuple([]byte{0, 0})

func NewTuple(pool pool.BuffPool, values ...[]byte) Tuple {
	var count int
	var pos ByteSize
	for _, v := range values {
		if isNull(v) {
			continue
		}
		count++
		pos += sizeOf(v)
	}
	if len(values) > MaxTupleFields {
		panic("tuple field maxIdx exceeds maximum")
	}
	if pos > MaxTupleDataSize {
		panic("tuple data size exceeds maximum")
	}

	tup, offs, mask := allocateTuple(pool, pos, count, len(values))

	count = 0
	pos = ByteSize(0)
	for i, v := range values {
		if isNull(v) {
			continue
		}
		mask.set(i)
		offs.putOffset(count, pos)
		count++

		copy(tup[pos:pos+sizeOf(v)], v)
		pos += sizeOf(v)
	}

	return tup
}

func CloneTuple(pool pool.BuffPool, tup Tuple) Tuple {
	buf := pool.Get(uint64(len(tup)))
	copy(buf, tup)
	return buf
}

func allocateTuple(pool pool.BuffPool, bufSz ByteSize, values, fields int) (tup Tuple, offs offsets, ms memberMask) {
	offSz := offsetsSize(values)
	maskSz := maskSize(fields)
	countSz := numFieldsSize

	tup = pool.Get(uint64(bufSz + offSz + maskSz + countSz))

	writeFieldCount(tup, fields)
	offs = offsets(tup[bufSz : bufSz+offSz])
	ms = memberMask(tup[bufSz+offSz : bufSz+offSz+maskSz])

	return
}

// GetField returns the value for field |i|.
func (tup Tuple) GetField(i int) []byte {
	// first check if the field is NULL
	if !tup.mask().present(i) {
		return nil
	}

	// translate from field index to value
	// index to compensate for NULL fields
	i = tup.fieldToValue(i)

	return slicedTupleBuffer(tup).GetSlice(i)
}

func (tup Tuple) size() ByteSize {
	return ByteSize(len(tup))
}

func (tup Tuple) Count() int {
	return tup.fieldCount()
}

func (tup Tuple) fieldCount() int {
	sl := tup[tup.size()-numFieldsSize:]
	return int(readUint16(sl))
}

func (tup Tuple) valueCount() int {
	return tup.mask().count()
}

func (tup Tuple) mask() memberMask {
	stop := tup.size() - numFieldsSize
	start := stop - maskSize(tup.fieldCount())
	return memberMask(tup[start:stop])
}

func (tup Tuple) fieldToValue(i int) int {
	return tup.mask().countPrefix(i) - 1
}

func isNull(val []byte) bool {
	return val == nil
}

func sizeOf(val []byte) ByteSize {
	return ByteSize(len(val))
}

func writeFieldCount(tup Tuple, count int) {
	sl := tup[len(tup)-int(numFieldsSize):]
	writeUint16(sl, uint16(count))
}
