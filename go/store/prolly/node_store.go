package prolly

import (
	"context"
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
)

const (
	cacheSize = 256 * 1024 * 1024
)

type NodeStore struct {
	cs    chunks.ChunkStore
	cache *chunkCache
	bp    pool.BuffPool
}

var sharedPool = pool.NewBuffPool()

func NewNodeStore(cs chunks.ChunkStore) NodeStore {
	return NodeStore{
		cs:    cs,
		cache: newChunkCache(cacheSize),
		bp:    sharedPool,
	}
}

func (ns NodeStore) Read(ctx context.Context, ref hash.Hash) (Node, error) {
	c, ok := ns.cache.get(ref)
	if ok {
		return c.Data(), nil
	}

	c, err := ns.cs.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	ns.cache.insert(c)

	return c.Data(), err
}

func (ns NodeStore) Write(ctx context.Context, nd Node) (hash.Hash, error) {
	c := chunks.NewChunk(nd)
	if err := ns.cs.Put(ctx, c); err != nil {
		return hash.Hash{}, err
	}
	ns.cache.insert(c)
	return c.Hash(), nil
}

func (ns NodeStore) Pool() pool.BuffPool {
	return ns.bp
}

type centry struct {
	c    chunks.Chunk
	i    int
	prev *centry
	next *centry
}

type chunkCache struct {
	mu     *sync.Mutex
	chunks map[hash.Hash]*centry
	head   *centry
	sz     int
	maxSz  int
	rev    int
}

func newChunkCache(maxSize int) *chunkCache {
	return &chunkCache{
		&sync.Mutex{},
		make(map[hash.Hash]*centry),
		nil,
		0,
		maxSize,
		0,
	}
}

func removeFromCList(e *centry) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = e
	e.next = e
}

func (mc *chunkCache) moveToFront(e *centry) {
	e.i = mc.rev
	mc.rev++
	if mc.head == e {
		return
	}
	if mc.head != nil {
		removeFromCList(e)
		e.next = mc.head
		e.prev = mc.head.prev
		mc.head.prev = e
		e.prev.next = e
	}
	mc.head = e
}

func (mc *chunkCache) get(h hash.Hash) (chunks.Chunk, bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if e, ok := mc.chunks[h]; ok {
		mc.moveToFront(e)
		return e.c, true
	} else {
		return chunks.EmptyChunk, false
	}
}

func (mc *chunkCache) getMany(hs hash.HashSet) ([]chunks.Chunk, hash.HashSet) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	absent := make(map[hash.Hash]struct{})
	var found []chunks.Chunk
	for h, _ := range hs {
		if e, ok := mc.chunks[h]; ok {
			mc.moveToFront(e)
			found = append(found, e.c)
		} else {
			absent[h] = struct{}{}
		}
	}
	return found, absent
}

func (mc *chunkCache) insert(c chunks.Chunk) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.addIfAbsent(c)
}

func (mc *chunkCache) insertMany(cs []chunks.Chunk) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for _, c := range cs {
		mc.addIfAbsent(c)
	}
}

func (mc *chunkCache) addIfAbsent(c chunks.Chunk) {
	if e, ok := mc.chunks[c.Hash()]; !ok {
		e := &centry{c, 0, nil, nil}
		e.next = e
		e.prev = e
		mc.moveToFront(e)
		mc.chunks[c.Hash()] = e
		mc.sz += c.Size()
		mc.shrinkToMaxSz()
	} else {
		mc.moveToFront(e)
	}
}

func (mc *chunkCache) Len() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return len(mc.chunks)
}

func (mc *chunkCache) Size() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.sz
}

func (mc *chunkCache) shrinkToMaxSz() {
	for mc.sz > mc.maxSz {
		if mc.head != nil {
			t := mc.head.prev
			removeFromCList(t)
			if t == mc.head {
				mc.head = nil
			}
			delete(mc.chunks, t.c.Hash())
			mc.sz -= t.c.Size()
		} else {
			panic("cache is empty but cache size is > than max size")
		}
	}
}

func (mc *chunkCache) sanityCheck() {
	if mc.head != nil {
		p := mc.head.next
		i := 1
		sz := mc.head.c.Size()
		lasti := mc.head.i
		for p != mc.head {
			i++
			sz += p.c.Size()
			if p.i >= lasti {
				panic("encountered lru list entry with higher rev later in the list.")
			}
			p = p.next
		}
		if i != len(mc.chunks) {
			panic(fmt.Sprintf("cache lru list has different size than cache.chunks. %d vs %d", i, len(mc.chunks)))
		}
		if sz != mc.sz {
			panic("entries reachable from lru list have different size than cache.sz.")
		}
		j := 1
		p = mc.head.prev
		for p != mc.head {
			j++
			p = p.prev
		}
		if j != i {
			panic("length of list backwards is not equal to length of list forward")
		}
	} else {
		if len(mc.chunks) != 0 {
			panic("lru list is empty but mc.chunks is not")
		}
		if mc.sz != 0 {
			panic("lru list is empty but mc.sz is not 0")
		}
	}
}
