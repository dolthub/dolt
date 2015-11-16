package types

import (
	"math/rand"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

type testSimpleList []Value

func (tsl testSimpleList) Get(idx uint64) Value {
	return tsl[idx]
}

func getTestSimpleList() testSimpleList {
	length := int(listPattern * 16)
	s := rand.NewSource(42)
	values := make([]Value, length, length)
	for i := 0; i < length; i++ {
		values[i] = Int64(s.Int63() & 0xff)
	}

	return values
}

func TestCompoundListGet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(MetaSequenceKind, MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind)))
	cl := NewCompoundList(tr, cs, simpleList...).(compoundList)

	for i, v := range simpleList {
		assert.Equal(v, cl.Get(uint64(i)))
	}
}

func TestCompoundListIter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(MetaSequenceKind, MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind)))
	cl := NewCompoundList(tr, cs, simpleList...).(compoundList)

	expectIdx := uint64(0)
	endAt := uint64(listPattern)
	cl.Iter(func(v Value, idx uint64) bool {
		assert.Equal(expectIdx, idx)
		expectIdx += 1
		assert.Equal(simpleList.Get(idx), v)
		if expectIdx == endAt {
			return true
		}
		return false
	})

	assert.Equal(endAt, expectIdx)
}

func TestCompoundListIterAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	simpleList := getTestSimpleList()
	tr := MakeCompoundType(MetaSequenceKind, MakeCompoundType(ListKind, MakePrimitiveType(Int64Kind)))
	cl := NewCompoundList(tr, cs, simpleList...).(compoundList)

	expectIdx := uint64(0)
	cl.IterAll(func(v Value, idx uint64) {
		assert.Equal(expectIdx, idx)
		expectIdx += 1
		assert.Equal(simpleList.Get(idx), v)
	})
}
