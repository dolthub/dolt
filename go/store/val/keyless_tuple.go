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

package val

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/zeebo/xxh3"

	"github.com/dolthub/dolt/go/store/pool"
)

const (
	keylessCardSz  = uint64Size
	keylessHashSz  = hash128Size
	keylessTupleSz = keylessHashSz + 2
)

func HashTupleFromValue(pool pool.BuffPool, value Tuple) (key Tuple) {
	if len(value) < 2 || value.FieldIsNull(0) {
		panic("invalid keyless value")
	}

	// omit the cardinality field from the hash id
	fields := value[keylessCardSz:]

	key = pool.Get(uint64(keylessTupleSz))
	h := xxh3.Hash128(fields)
	binary.LittleEndian.PutUint64(key[0:8], h.Lo)
	binary.LittleEndian.PutUint64(key[8:keylessHashSz], h.Hi)
	copy(key[keylessHashSz:], keySuffix[:])
	return
}

func ReadHashFromTuple(keylessKey Tuple) []byte {
	return keylessKey[keylessHashSz:]
}

func ReadKeylessCardinality(value Tuple) uint64 {
	return readUint64(value[:keylessCardSz])
}

func ModifyKeylessCardinality(pool pool.BuffPool, value Tuple, delta int64) (updated Tuple, after uint64) {
	updated = cloneTuple(pool, value)
	buf := updated[:keylessCardSz]
	after = uint64(int64(readUint64(buf)) + delta)
	writeUint64(buf, after)
	return
}

// field count of 1, little endian encoded
var keySuffix = [...]byte{1, 0}

var KeylessTupleDesc = TupleDesc{
	Types: []Type{{Enc: Hash128Enc, Nullable: false}},
	cmp:   keylessCompare{},
}

var KeylessCardType = Type{
	Enc:      Uint64Enc,
	Nullable: false,
}

type keylessCompare struct{}

var _ TupleComparator = keylessCompare{}

// Compare implements TupleComparator
func (k keylessCompare) Compare(ctx context.Context, left, right Tuple, desc TupleDesc) int {
	return bytes.Compare(left, right)
}

// CompareValues implements TupleComparator
func (k keylessCompare) CompareValues(ctx context.Context, index int, left, right []byte, typ Type) int {
	return compare(typ, left, right)
}

// Prefix implements TupleComparator
func (k keylessCompare) Prefix(n int) TupleComparator {
	return k
}

// Suffix implements TupleComparator
func (k keylessCompare) Suffix(n int) TupleComparator {
	return k
}

func (k keylessCompare) Validated(types []Type) TupleComparator {
	return k
}
