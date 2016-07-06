// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"sync"

	"github.com/attic-labs/noms/go/hash"
)

func newUnwrittenPutCache() *unwrittenPutCache {
	return &unwrittenPutCache{map[hash.Hash]Chunk{}, &sync.Mutex{}}
}

type unwrittenPutCache struct {
	unwrittenPuts map[hash.Hash]Chunk
	mu            *sync.Mutex
}

func (p *unwrittenPutCache) Add(c Chunk) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.unwrittenPuts[c.Hash()]; !ok {
		p.unwrittenPuts[c.Hash()] = c
		return true
	}

	return false
}

func (p *unwrittenPutCache) Has(c Chunk) (has bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, has = p.unwrittenPuts[c.Hash()]
	return
}

func (p *unwrittenPutCache) Get(r hash.Hash) Chunk {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.unwrittenPuts[r]; ok {
		return c
	}
	return EmptyChunk
}

func (p *unwrittenPutCache) Clear(chunks []Chunk) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range chunks {
		delete(p.unwrittenPuts, c.Hash())
	}
}
