package chunks

import (
	"sync"

	"github.com/attic-labs/noms/ref"
)

func newUnwrittenPutCache() *unwrittenPutCache {
	return &unwrittenPutCache{map[ref.Ref]Chunk{}, &sync.Mutex{}}
}

type unwrittenPutCache struct {
	unwrittenPuts map[ref.Ref]Chunk
	mu            *sync.Mutex
}

func (p *unwrittenPutCache) Add(c Chunk) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.unwrittenPuts[c.Ref()]; !ok {
		p.unwrittenPuts[c.Ref()] = c
		return true
	}

	return false
}

func (p *unwrittenPutCache) Get(r ref.Ref) Chunk {
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
		delete(p.unwrittenPuts, c.Ref())
	}
}
