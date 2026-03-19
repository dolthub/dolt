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

package pool

import "sync"

const (
	// slabSize is the size of each slab allocated by the pool.
	// 16KB balances allocation amortization against memory waste.
	slabSize = 16 * 1024

	// largeAllocSize is the threshold above which allocations
	// bypass the slab and go directly to make(). Set to 1/4 of
	// slabSize so a single allocation can't burn more than 25%
	// of a slab.
	largeAllocSize = slabSize / 4
)

// BuffPool allocates byte slices. Implementations may use
// pooling or arena strategies to reduce allocation overhead.
type BuffPool interface {
	Get(size uint64) []byte
	GetSlices(size uint64) [][]byte
}

// slabPool is a bump allocator that sub-allocates small requests
// from larger slabs. This reduces the number of individual heap
// objects, lowering GC pressure compared to per-request make()
// calls. Allocated byte slices are not returned to the pool;
// the slab becomes eligible for GC when all of its sub-slices
// are unreferenced.
type slabPool struct {
	mu     sync.Mutex
	slab   []byte
	offset int
}

// NewBuffPool returns a BuffPool backed by a slab allocator.
func NewBuffPool() BuffPool {
	return &slabPool{}
}

// Get returns a byte slice of the requested size. Small requests
// are sub-allocated from a shared slab; large requests fall
// through to make().
func (sp *slabPool) Get(size uint64) []byte {
	sz := int(size)
	if sz == 0 {
		return make([]byte, 0)
	}
	if sz > largeAllocSize {
		return make([]byte, sz)
	}

	sp.mu.Lock()
	if sp.offset+sz > len(sp.slab) {
		sp.slab = make([]byte, slabSize)
		sp.offset = 0
	}
	buf := sp.slab[sp.offset : sp.offset+sz : sp.offset+sz]
	sp.offset += sz
	sp.mu.Unlock()
	return buf
}

// GetSlices returns a [][]byte of the requested length.
func (sp *slabPool) GetSlices(size uint64) [][]byte {
	return make([][]byte, size)
}
