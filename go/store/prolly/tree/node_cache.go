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
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	numStripes = 256
)

func newChunkCache(maxSize int) (c chunkCache) {
	sz := maxSize / numStripes
	for i := range c.stripes {
		c.stripes[i] = newStripe(sz)
	}
	return
}

type chunkCache struct {
	stripes [numStripes]*stripe
}

func (c chunkCache) get(addr hash.Hash) (chunks.Chunk, bool) {
	return c.pickStripe(addr).get(addr)
}

func (c chunkCache) insert(ch chunks.Chunk) {
	c.pickStripe(ch.Hash()).insert(ch)
}

func (c chunkCache) pickStripe(addr hash.Hash) *stripe {
	i := binary.LittleEndian.Uint32(addr[:4]) % numStripes
	return c.stripes[i]
}

type centry struct {
	c    chunks.Chunk
	i    int
	prev *centry
	next *centry
}

type stripe struct {
	mu     *sync.Mutex
	chunks map[hash.Hash]*centry
	head   *centry
	sz     int
	maxSz  int
	rev    int
}

func newStripe(maxSize int) *stripe {
	return &stripe{
		&sync.Mutex{},
		make(map[hash.Hash]*centry),
		nil,
		0,
		maxSize,
		0,
	}
}

func removeFromList(e *centry) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = e
	e.next = e
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

func (s *stripe) get(h hash.Hash) (chunks.Chunk, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.chunks[h]; ok {
		s.moveToFront(e)
		return e.c, true
	} else {
		return chunks.EmptyChunk, false
	}
}

func (s *stripe) insert(c chunks.Chunk) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addIfAbsent(c)
}

func (s *stripe) addIfAbsent(c chunks.Chunk) {
	if e, ok := s.chunks[c.Hash()]; !ok {
		e = &centry{c, 0, nil, nil}
		e.next = e
		e.prev = e
		s.moveToFront(e)
		s.chunks[c.Hash()] = e
		s.sz += c.Size()
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
			delete(s.chunks, t.c.Hash())
			s.sz -= t.c.Size()
		} else {
			panic("cache is empty but cache Size is > than max Size")
		}
	}
}

func (s *stripe) sanityCheck() {
	if s.head != nil {
		p := s.head.next
		i := 1
		sz := s.head.c.Size()
		lasti := s.head.i
		for p != s.head {
			i++
			sz += p.c.Size()
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
