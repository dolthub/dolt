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
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	numStripes = 256
)

func newChunkCache(maxSize int) (c nodeCache) {
	sz := maxSize / numStripes
	for i := range c.stripes {
		c.stripes[i] = newStripe(sz)
	}
	return
}

type nodeCache struct {
	stripes [numStripes]*lru.TwoQueueCache[hash.Hash, Node]
}

func (c nodeCache) get(addr hash.Hash) (Node, bool) {
	s := c.stripes[addr[0]]
	return s.Get(addr)
}

func (c nodeCache) insert(addr hash.Hash, node Node) {
	s := c.stripes[addr[0]]
	s.Add(addr, node)
}

func (c nodeCache) purge() {
	for _, s := range c.stripes {
		s.Purge()
	}
}

func newStripe(maxSize int) *lru.TwoQueueCache[hash.Hash, Node] {
	c, err := lru.New2Q[hash.Hash, Node](maxSize)
	if err != nil {
		panic(err) // A good reason to die. Not enough memory to allocate cache.
	}
	return c
}
