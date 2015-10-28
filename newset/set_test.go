package newset

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
)

func TestEmpty(t *testing.T) {
	assert := assert.New(t)
	assertIsEmpty(assert, newSetForSetTest())
}

func TestPutInAscendingOrder(t *testing.T) {
	assert := assert.New(t)
	ator := newReferrator()

	empty := newSetForSetTest()
	r0, r1, r2 := ator.Next(), ator.Next(), ator.Next()

	set := empty.Put(r0)
	assert.Equal(uint64(1), set.Len())
	assert.True(set.Has(r0))

	// Putting r0 again shouldn't affect set.
	set = set.Put(r0)
	assert.Equal(uint64(1), set.Len())
	assert.True(set.Has(r0))

	assert.False(set.Has(r1))
	set = set.Put(r1)
	assert.Equal(uint64(2), set.Len())
	assertHas(assert, set, r0, r1)

	// Putting previous values again shouldn't affect set.
	set = set.Put(r0).Put(r1)
	assert.Equal(uint64(2), set.Len())
	assertHas(assert, set, r0, r1)

	assert.False(set.Has(r2))
	set = set.Put(r2)
	assert.Equal(uint64(3), set.Len())
	assertHas(assert, set, r0, r1, r2)

	// Putting previous values again shouldn't affect set.
	set = set.Put(r0).Put(r1).Put(r2)
	assert.Equal(uint64(3), set.Len())
	assertHas(assert, set, r0, r1, r2)

	// Check that Put didn't modify a previous set.
	assertIsEmpty(assert, empty)
}

func TestPutInDescendingOrder(t *testing.T) {
	assert := assert.New(t)
	ator := newReferrator()

	empty := newSetForSetTest()
	r0, r1, r2 := ator.Next(), ator.Next(), ator.Next()

	set := empty.Put(r2)
	assert.Equal(uint64(1), set.Len())
	assert.True(set.Has(r2))

	// Putting r2 again shouldn't affect set.
	set = set.Put(r2)
	assert.Equal(uint64(1), set.Len())
	assert.True(set.Has(r2))

	assert.False(set.Has(r1))
	set = set.Put(r1)
	assert.Equal(uint64(2), set.Len())
	assertHas(assert, set, r2, r1)

	// Putting previous values again shouldn't affect set.
	set = set.Put(r2).Put(r1)
	assert.Equal(uint64(2), set.Len())
	assertHas(assert, set, r2, r1)

	assert.False(set.Has(r0))
	set = set.Put(r0)
	assert.Equal(uint64(3), set.Len())
	assertHas(assert, set, r2, r1, r0)

	// Putting previous values again shouldn't affect set.
	set = set.Put(r2).Put(r1).Put(r0)
	assert.Equal(uint64(3), set.Len())
	assertHas(assert, set, r2, r1, r0)

	// Check that Put didn't modify a previous set.
	assertIsEmpty(assert, empty)
}

func TestPutInMiddle(t *testing.T) {
	assert := assert.New(t)
	ator := newReferrator()

	empty := newSetForSetTest()
	r0, r1, r2 := ator.Next(), ator.Next(), ator.Next()

	set := empty.Put(r0).Put(r2)
	assertHas(assert, set, r0, r2)

	assert.False(set.Has(r1))
	set = set.Put(r1)
	assert.Equal(uint64(3), set.Len())
	assertHas(assert, set, r0, r2, r1)

	// Putting previous values again shouldn't affect set.
	set = set.Put(r0).Put(r2).Put(r1)
	assert.Equal(uint64(3), set.Len())
	assertHas(assert, set, r0, r2, r1)

	// Check that Put didn't modify a previous set.
	assertIsEmpty(assert, empty)
}

func assertIsEmpty(assert *assert.Assertions, s Set) {
	ator := newReferrator()
	assert.Equal(uint64(0), s.Len())
	assert.False(s.Has(ref.Ref{}))
	assert.False(s.Has(ator.Next()))
	assert.False(s.Has(ator.Next()))
	assert.Equal(ref.Ref{}, s.Ref()) // this will change when we serialize newsets
	assert.Equal("(empty set)", s.Fmt())
}

func assertHas(assert *assert.Assertions, s Set, rs ...ref.Ref) {
	for _, r := range rs {
		assert.True(s.Has(r))
	}
}

func newSetForSetTest() Set {
	store := newNodeStore()
	// Note that 2 is a good value because these tests all deal with 3 values, therefore 2 will trigger a chunk.
	return NewSet(&store, newFixedSizeChunkerFactory(2))
}
