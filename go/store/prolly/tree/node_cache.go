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

package tree

import (
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	numStripes = 256
)

type nodeCache []*stripe

func newChunkCache(maxSize int) nodeCache {
	cache := make(nodeCache, numStripes)
	sz := maxSize / numStripes
	for i := range cache {
		cache[i] = newStripe(sz)
	}
	return cache
}

// get returns the cached node for |addr|, if there is one, along with
// the generation of the stripe |addr| belongs to. A caller which misses
// here and goes to the ChunkStore must pass that generation back to
// |insert|.
func (c nodeCache) get(addr hash.Hash) (*Node, uint64, bool) {
	return c[addr[0]].get(addr)
}

// generation returns the current generation of the stripe |addr|
// belongs to, for callers which have not just missed in |get|.
func (c nodeCache) generation(addr hash.Hash) uint64 {
	return c[addr[0]].generation()
}

// insert caches |node| under |addr|, unless the cache has been purged
// since |gen| was read.
func (c nodeCache) insert(addr hash.Hash, node *Node, gen uint64) {
	c[addr[0]].insert(addr, node, gen)
}

func (c nodeCache) purge() {
	for i := range c {
		c[i].purge()
	}
}

type centry struct {
	prev *centry
	next *centry
	n    *Node
	i    int
	a    hash.Hash
}

type stripe struct {
	mu     *sync.Mutex
	chunks map[hash.Hash]*centry
	head   *centry
	sz     int
	maxSz  int
	rev    int

	// Incremented by every purge. Read out of get and rechecked by
	// insert, both under mu, so that a node which was fetched from the
	// ChunkStore before a purge cannot be cached after it.
	//
	// The read which fetched it took no GC read dependency on it, so
	// caching it would hand out a node the GC is free to collect.
	gen uint64
}

func newStripe(maxSize int) *stripe {
	return &stripe{
		mu:     &sync.Mutex{},
		chunks: make(map[hash.Hash]*centry),
		maxSz:  maxSize,
	}
}

func removeFromList(e *centry) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = e
	e.next = e
}

func (s *stripe) purge() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = make(map[hash.Hash]*centry)
	s.head = nil
	s.sz = 0
	s.rev = 0
	s.gen += 1
}

func (s *stripe) generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen
}

func (s *stripe) moveToFront(e *centry) {
	e.i = s.rev
	s.rev++
	if s.head == e {
		return
	}
	if s.head != nil {
		removeFromList(e)
		e.next = s.head
		e.prev = s.head.prev
		s.head.prev = e
		e.prev.next = e
	}
	s.head = e
}

func (s *stripe) get(h hash.Hash) (*Node, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.chunks[h]; ok {
		s.moveToFront(e)
		return e.n, s.gen, true
	} else {
		return nil, s.gen, false
	}
}

func (s *stripe) insert(addr hash.Hash, node *Node, gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.gen != gen {
		return
	}

	if e, ok := s.chunks[addr]; !ok {
		e = &centry{nil, nil, node, 0, addr}
		e.next = e
		e.prev = e
		s.moveToFront(e)
		s.chunks[addr] = e
		s.sz += node.Size()
		s.shrinkToMaxSz()
	} else {
		s.moveToFront(e)
	}
}

func (s *stripe) shrinkToMaxSz() {
	for s.sz > s.maxSz {
		if s.head != nil {
			t := s.head.prev
			removeFromList(t)
			if t == s.head {
				s.head = nil
			}
			delete(s.chunks, t.a)
			s.sz -= t.n.Size()
		} else {
			panic("cache is empty but cache Size is > than max Size")
		}
	}
}

func (s *stripe) sanityCheck() {
	if s.head != nil {
		p := s.head.next
		i := 1
		sz := s.head.n.Size()
		lasti := s.head.i
		for p != s.head {
			i++
			sz += p.n.Size()
			if p.i >= lasti {
				panic("encountered lru list entry with higher rev later in the list.")
			}
			p = p.next
		}
		if i != len(s.chunks) {
			panic(fmt.Sprintf("cache lru list has different Size than cache.chunks. %d vs %d", i, len(s.chunks)))
		}
		if sz != s.sz {
			panic("entries reachable from lru list have different Size than cache.sz.")
		}
		j := 1
		p = s.head.prev
		for p != s.head {
			j++
			p = p.prev
		}
		if j != i {
			panic("length of list backwards is not equal to length of list forward")
		}
	} else {
		if len(s.chunks) != 0 {
			panic("lru list is empty but s.chunks is not")
		}
		if s.sz != 0 {
			panic("lru list is empty but s.sz is not 0")
		}
	}
}
