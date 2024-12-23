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

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

const (
	MaxTupleFields          = 4096
	countSize      ByteSize = 2
	nodeCountSize           = uint64Size
	treeLevelSize           = uint8Size

	// MaxTupleDataSize is the maximum KV length considering the extra
	// flatbuffer metadata required to serialize the message. This number
	// implicitly checks the "last row" size that will append chunk level
	// metadata. Key and value offsets per field are excluded from this number.
	// (uint16) - (field count) - (content hash) - (node count) - (tree level)
	MaxTupleDataSize ByteSize = math.MaxUint16 - countSize - hash.ByteLen - nodeCountSize - treeLevelSize
)

// A Tuple is a vector of fields encoded as a byte slice. Key-Value Tuple pairs
// are used to store row data within clustered and secondary indexes in Dolt.
//
// The encoding format for Tuples starts with field values packed contiguously from
// the front of the Tuple, followed by field offsets, and finally a field count:
//
//	+---------+---------+-----+---------+----------+-----+----------+-------+
//	| Value 0 | Value 1 | ... | Value K | Offset 1 | ... | Offset K | Count |
//	+---------+---------+-----+---------+----------+-----+----------+-------+
//
// Field offsets encode the byte-offset from the front of the Tuple to the beginning
// of the corresponding field in the Tuple. The offset for the first field is always
// zero and is therefore omitted. Offsets and the field count are little-endian
// encoded uint16 values.
//
// Tuples read and write field values as byte slices. Interpreting these encoded
// values is left up to TupleDesc which knows about a Tuple's schema and associated
// field encodings. Zero-length fields are interpreted as NULL values, all non-NULL
// values must be encoded with non-zero length. For this reason, variable-length
// strings are encoded with a NUL terminator (see codec.go).
//
// Accessing the ith field where i > count will return a NULL value. This allows us
// to implicitly add nullable columns to the end of a schema without needing to
// rewrite index storage. However, because Dolt storage in content-addressed, we
// must have a single canonical encoding for any given Tuple. For this reason, the
// NULL suffix of a Tuple is explicitly truncated and the field count reduced.
type Tuple []byte

var EmptyTuple = Tuple([]byte{0, 0})

func NewTuple(pool pool.BuffPool, values ...[]byte) Tuple {
	values = trimNullSuffix(values)

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

	tup, offs := allocateTuple(pool, pos, len(values))

	count = 0
	pos = ByteSize(0)
	for _, v := range values {
		writeOffset(count, pos, offs)
		count++

		if isNull(v) {
			continue
		}

		copy(tup[pos:pos+sizeOf(v)], v)
		pos += sizeOf(v)
	}

	return tup
}

func trimNullSuffix(values [][]byte) [][]byte {
	n := len(values)
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] != nil {
			break
		}
		n--
	}
	return values[:n]
}

func cloneTuple(pool pool.BuffPool, tup Tuple) Tuple {
	buf := pool.Get(uint64(len(tup)))
	copy(buf, tup)
	return buf
}

func allocateTuple(pool pool.BuffPool, bufSz ByteSize, fields int) (tup Tuple, offs offsets) {
	offSz := offsetsSize(fields)
	tup = pool.Get(uint64(bufSz + offSz + countSize))
	writeFieldCount(tup, fields)
	offs = offsets(tup[bufSz : bufSz+offSz])

	return
}

func (tup Tuple) GetOffset(i int) (int, bool) {
	cnt := tup.Count()
	if i >= cnt {
		return 0, false
	}

	sz := ByteSize(len(tup))
	split := sz - uint16Size*ByteSize(cnt)
	offs := tup[split : sz-countSize]

	start, stop := uint16(0), uint16(split)
	if i*2 < len(offs) {
		pos := i * 2
		stop = ReadUint16(offs[pos : pos+2])
	}
	if i > 0 {
		pos := (i - 1) * 2
		start = ReadUint16(offs[pos : pos+2])
	}

	return int(start), start != stop
}

// GetField returns the value for field |i|.
func (tup Tuple) GetField(i int) []byte {
	cnt := tup.Count()
	if i >= cnt {
		return nil
	}

	sz := ByteSize(len(tup))
	split := sz - uint16Size*ByteSize(cnt)
	offs := tup[split : sz-countSize]

	start, stop := uint16(0), uint16(split)
	if i*2 < len(offs) {
		pos := i * 2
		stop = ReadUint16(offs[pos : pos+2])
	}
	if i > 0 {
		pos := (i - 1) * 2
		start = ReadUint16(offs[pos : pos+2])
	}

	if start == stop {
		return nil // NULL
	}

	return tup[start:stop]
}

func (tup Tuple) FieldIsNull(i int) bool {
	return tup.GetField(i) == nil
}

func (tup Tuple) Count() int {
	sl := tup[len(tup)-int(countSize):]
	return int(ReadUint16(sl))
}

func isNull(val []byte) bool {
	return val == nil
}

func sizeOf(val []byte) ByteSize {
	return ByteSize(len(val))
}

func writeFieldCount(tup Tuple, count int) {
	sl := tup[len(tup)-int(countSize):]
	WriteUint16(sl, uint16(count))
}

type offsets []byte

// offsetsSize returns the number of bytes needed to
// store |fieldCount| offsets.
func offsetsSize(count int) ByteSize {
	if count == 0 {
		return 0
	}
	return ByteSize((count - 1) * 2)
}

// writeOffset writes offset |pos| at index |i|.
func writeOffset(i int, off ByteSize, arr offsets) {
	if i == 0 {
		return
	}
	start := (i - 1) * 2
	WriteUint16(arr[start:start+2], uint16(off))
}
