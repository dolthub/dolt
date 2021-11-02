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

// nullMask is a bit-array encoding a NULL bitmask.
// NULLs are encoded as 0, non-NULLs as 1.
type nullMask []byte

func makeNullMask(pool pool.BuffPool, count int) nullMask {
	sz := uint64(maskSize(count))
	return pool.Get(sz)
}

// maskSize returns the ByteSize of a mask with |Count| members.
func maskSize(count int) ByteSize {
	return ByteSize((count + 7) / 8)
}

// size returns the byte size of |nm|
func (nm nullMask) size() ByteSize {
	return ByteSize(len(nm))
}

// set flips bit |i| to 1
func (nm nullMask) set(i int) {
	nm[i/8] |= uint8(1) << (i % 8)
}

// set flips bit |i| to 0
func (nm nullMask) unset(i int) {
	nm[i/8] &= ^(uint8(1) << (i % 8))
}

// present returns true if the |i|th member is non-null.
func (nm nullMask) present(i int) bool {
	query := uint8(1) << (i % 8)
	return query&nm[i/8] == query
}

// count returns the number of members present
func (nm nullMask) count() (n int) {
	for _, b := range nm {
		n += countBitsSet(b)
	}
	return
}

// countPrefix returns the count of the members at or before |i|.
func (nm nullMask) countPrefix(i int) (n int) {
	for _, b := range nm[:i/8] {
		n += countBitsSet(b)
	}
	n += countBitsSet(nm[i/8] & prefixMask(i%8))
	return
}

// countSuffix returns the count of the members at or after |i|.
func (nm nullMask) countSuffix(i int) (n int) {
	n += countBitsSet(nm[i/8] & suffixMask(i%8))
	for i := int(i/8) + 1; i < len(nm); i++ {
		n += countBitsSet(nm[i])
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
