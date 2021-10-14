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

import "github.com/dolthub/dolt/go/store/pool"

type memberSet []byte

func makeMemberSet(pool pool.BuffPool, count int) memberSet {
	sz := uint64(maskSize(count))
	return pool.Get(sz)
}

// maskSize returns the byteSize of a mask with |count| members.
func maskSize(count int) byteSize {
	return byteSize((count + 7) / 8)
}

func (ms memberSet) size() byteSize {
	return byteSize(len(ms))
}

func (ms memberSet) set(i int) {
	ms[i/8] |= uint8(1) << (i % 8)
}

func (ms memberSet) unset(i int) {
	ms[i/8] &= ^(uint8(1) << (i % 8))
}

// present returns true if the |i|th member is non-null.
func (ms memberSet) present(i int) bool {
	query := uint8(1) << (i % 8)
	return query&ms[i/8] == query
}

// count returns the number of members present
func (ms memberSet) count() (n int) {
	for _, b := range ms {
		n += countBitsSet(b)
	}
	return
}

// countPrefix returns the count of the members at or before |i|.
func (ms memberSet) countPrefix(i int) (n int) {
	for _, b := range ms[:i/8] {
		n += countBitsSet(b)
	}
	n += countBitsSet(ms[i/8] & prefixMask(i%8))
	return
}

// countSuffix returns the count of the members at or after |i|.
func (ms memberSet) countSuffix(i int) (n int) {
	n += countBitsSet(ms[i/8] & suffixMask(i%8))
	for i := int(i/8) + 1; i < len(ms); i++ {
		n += countBitsSet(ms[i])
	}
	return
}

func prefixMask(k int) byte {
	return byte(255) >> (7 - k)
}

func suffixMask(k int) byte {
	return byte(255) << k
}

func countBitsSet(b uint8) (n int) {
	n += int(1 & b)
	b >>= 1
	n += int(1 & b)
	b >>= 1
	n += int(1 & b)
	b >>= 1
	n += int(1 & b)
	b >>= 1
	n += int(1 & b)
	b >>= 1
	n += int(1 & b)
	b >>= 1
	n += int(1 & b)
	b >>= 1
	n += int(1 & b)
	return
}
