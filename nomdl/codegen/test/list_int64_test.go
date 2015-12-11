package test

import (
	"sync"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
)

func TestListInt64Def(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	def := gen.ListOfInt64Def{}
	l := def.New(cs)

	def2 := l.Def()
	l2 := def.New(cs)

	assert.Equal(def, def2)
	assert.True(l.Equals(l2))

	l3 := gen.NewListOfInt64(cs)
	assert.True(l.Equals(l3))

	def3 := gen.ListOfInt64Def{0, 1, 2, 3, 4}
	l4 := def3.New(cs)
	assert.Equal(uint64(5), l4.Len())
	assert.Equal(int64(0), l4.Get(0))
	assert.Equal(int64(2), l4.Get(2))
	assert.Equal(int64(4), l4.Get(4))

	l4 = l4.Set(4, 44).Slice(3, 5)
	assert.Equal(gen.ListOfInt64Def{3, 44}, l4.Def())
}

func TestListIter(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	l := gen.ListOfInt64Def{0, 1, 2, 3, 4}.New(cs)
	acc := gen.ListOfInt64Def{}
	i := uint64(0)
	l.Iter(func(v int64, index uint64) (stop bool) {
		assert.Equal(i, index)
		stop = v == 2
		acc = append(acc, v)
		i++
		return
	})
	assert.Equal(gen.ListOfInt64Def{0, 1, 2}, acc)
}

func TestListIterAllP(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	l := gen.ListOfInt64Def{0, 1, 2, 3, 4}
	mu := sync.Mutex{}
	visited := map[int64]bool{}
	l.New(cs).IterAllP(2, func(v int64, index uint64) {
		assert.EqualValues(v, index)
		mu.Lock()
		defer mu.Unlock()
		visited[v] = true
	})

	if assert.Len(visited, len(l)) {
		for _, e := range l {
			assert.True(visited[e])
		}
	}
}

// Bug
func SkipTestListFilter(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	l := gen.ListOfInt64Def{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}.New(cs)
	i := uint64(0)
	l2 := l.Filter(func(v int64, index uint64) bool {
		assert.Equal(i, index)
		i++
		return v%2 == 0
	})
	assert.Equal(gen.ListOfInt64Def{0, 2, 4, 6, 8}, l2.Def())
}

func SkipTestListChunks(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	l := gen.ListOfInt64Def{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}.New(cs)
	chunks := l.Chunks()
	assert.Len(chunks, 0)
}
