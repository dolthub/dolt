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
	"math/bits"

	"github.com/dolthub/dolt/go/store/pool"
)

// memberMask is a bit-array encoding field membership in Tuples.
// Fields with non-NULL values are present, and encoded as 1,
// NULL fields are absent and encoded as 0.
type memberMask []byte

func makeMemberMask(pool pool.BuffPool, count int) memberMask {
	sz := uint64(maskSize(count))
	return pool.Get(sz)
}

// maskSize returns the ByteSize of a mask with |Count| members.
func maskSize(count int) ByteSize {
	return ByteSize((count + 7) / 8)
}

// size returns the byte size of |nm|
func (nm memberMask) size() ByteSize {
	return ByteSize(len(nm))
}

// set flips bit |i| to 1
func (nm memberMask) set(i int) {
	nm[i/8] |= uint8(1) << (i % 8)
}

// set flips bit |i| to 0
func (nm memberMask) unset(i int) {
	nm[i/8] &= ^(uint8(1) << (i % 8))
}

// present returns true if the |i|th member is non-null.
func (nm memberMask) present(i int) bool {
	query := uint8(1) << (i % 8)
	return query&nm[i/8] == query
}

// count returns the number of members present
func (nm memberMask) count() (n int) {
	for _, b := range nm {
		n += bits.OnesCount8(b)
	}
	return
}

// countPrefix returns the count of the members at or before |i|.
func (nm memberMask) countPrefix(i int) (n int) {
	for _, b := range nm[:i/8] {
		n += bits.OnesCount8(b)
	}
	n += bits.OnesCount8(nm[i/8] & prefixMask(i%8))
	return
}

// countSuffix returns the count of the members at or after |i|.
func (nm memberMask) countSuffix(i int) (n int) {
	n += bits.OnesCount8(nm[i/8] & suffixMask(i%8))
	for i := int(i/8) + 1; i < len(nm); i++ {
		n += bits.OnesCount8(nm[i])
	}
	return
}

func prefixMask(k int) byte {
	return byte(255) >> (7 - k)
}

func suffixMask(k int) byte {
	return byte(255) << k
}
