package datas

import (
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func newUnwrittenPutCache() *unwrittenPutCache {
	return &unwrittenPutCache{map[ref.Ref]chunks.Chunk{}, &sync.Mutex{}}
}

type unwrittenPutCache struct {
	unwrittenPuts map[ref.Ref]chunks.Chunk
	mu            *sync.Mutex
}

func (p *unwrittenPutCache) Add(c chunks.Chunk) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.unwrittenPuts[c.Ref()]; !ok {
		p.unwrittenPuts[c.Ref()] = c
		return true
	}

	return false
}

func (p *unwrittenPutCache) Has(c chunks.Chunk) (has bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, has = p.unwrittenPuts[c.Ref()]
	return
}

func (p *unwrittenPutCache) Get(r ref.Ref) chunks.Chunk {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.unwrittenPuts[r]; ok {
		return c
	}
	return chunks.EmptyChunk
}

func (p *unwrittenPutCache) Clear(hashes ref.RefSlice) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, hash := range hashes {
		delete(p.unwrittenPuts, hash)
	}
}
