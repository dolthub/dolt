package test

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/nomdl/codegen/test/gen"
)

func TestListInt64Def(t *testing.T) {
	assert := assert.New(t)

	def := gen.ListOfInt64Def{}
	l := def.New()

	def2 := l.Def()
	l2 := def.New()

	assert.Equal(def, def2)
	assert.True(l.Equals(l2))

	l3 := gen.NewListOfInt64()
	assert.True(l.Equals(l3))

	def3 := gen.ListOfInt64Def{0, 1, 2, 3, 4}
	l4 := def3.New()
	assert.Equal(uint64(5), l4.Len())
	assert.Equal(int64(0), l4.Get(0))
	assert.Equal(int64(2), l4.Get(2))
	assert.Equal(int64(4), l4.Get(4))

	l4 = l4.Set(4, 44).Slice(3, 5)
	assert.Equal(gen.ListOfInt64Def{3, 44}, l4.Def())
}

func TestListIter(t *testing.T) {
	assert := assert.New(t)
	l := gen.ListOfInt64Def{0, 1, 2, 3, 4}.New()
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

func TestListFilter(t *testing.T) {
	assert := assert.New(t)
	l := gen.ListOfInt64Def{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}.New()
	i := uint64(0)
	l2 := l.Filter(func(v int64, index uint64) bool {
		assert.Equal(i, index)
		i++
		return v%2 == 0
	})
	assert.Equal(gen.ListOfInt64Def{0, 2, 4, 6, 8}, l2.Def())
}

func TestListChunks(t *testing.T) {
	assert := assert.New(t)

	l := gen.ListOfInt64Def{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}.New()
	cs := l.Chunks()
	assert.Len(cs, 0)
}
