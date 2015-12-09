package types

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

type testNativeOrderSet []Value

func (tss testNativeOrderSet) Len() int {
	return len(tss)
}

func (tss testNativeOrderSet) Less(i, j int) bool {
	return tss[i].(OrderedValue).Less(tss[j].(OrderedValue))
}

func (tss testNativeOrderSet) Swap(i, j int) {
	tss[i], tss[j] = tss[j], tss[i]
}

func getTestNativeOrderSet() []Value {
	length := int(setPattern * 16)
	s := rand.NewSource(42)
	used := map[int64]bool{}

	values := testNativeOrderSet{}
	for len(values) < length {
		v := s.Int63() & 0xffffff
		if _, ok := used[v]; !ok {
			values = append(values, Int64(v))
			used[v] = true
		}
	}

	sort.Sort(values)
	return values
}

type testRefOrderSet []Value

func (tss testRefOrderSet) Len() int {
	return len(tss)
}

func (tss testRefOrderSet) Less(i, j int) bool {
	return tss[i].Ref().Less(tss[j].Ref())
}

func (tss testRefOrderSet) Swap(i, j int) {
	tss[i], tss[j] = tss[j], tss[i]
}

func getTestRefOrderSet() []Value {
	length := int(setPattern * 2)
	s := rand.NewSource(42)
	used := map[int64]bool{}

	values := testRefOrderSet{}
	for i := 0; i < length; i++ {
		v := s.Int63() & 0xffffff
		if _, ok := used[v]; !ok {
			values = append(values, NewRef(Int64(v).Ref()))
			used[v] = true
		}
	}

	sort.Sort(values)
	return values
}

func TestCompoundSetHas(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	doTest := func(simpleSet []Value, set compoundSet) {
		for _, v := range simpleSet {
			assert.True(set.Has(v))
		}
	}

	simpleSet := getTestNativeOrderSet()
	tr := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	set := NewTypedSet(cs, tr, simpleSet...).(compoundSet)
	doTest(simpleSet, set)

	simpleSet = getTestRefOrderSet()
	tr = MakeCompoundType(SetKind, MakeCompoundType(RefKind, MakePrimitiveType(ValueKind)))
	set = NewTypedSet(cs, tr, simpleSet...).(compoundSet)
	doTest(simpleSet, set)
}

func TestCompoundSetIter(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	doTest := func(simpleSet []Value, set compoundSet) {
		idx := uint64(0)
		endAt := uint64(setPattern)

		set.Iter(func(v Value) bool {
			assert.True(simpleSet[idx].Equals(v))
			if idx == endAt {
				idx += 1
				return true
			}

			idx += 1
			return false
		})

		assert.Equal(endAt, idx-1)
	}

	simpleSet := getTestNativeOrderSet()
	tr := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	set := NewTypedSet(cs, tr, simpleSet...).(compoundSet)
	doTest(simpleSet, set)

	simpleSet = getTestRefOrderSet()
	tr = MakeCompoundType(SetKind, MakeCompoundType(RefKind, MakePrimitiveType(ValueKind)))
	set = NewTypedSet(cs, tr, simpleSet...).(compoundSet)
	doTest(simpleSet, set)
}

func TestCompoundSetIterAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	doTest := func(simpleSet []Value, set compoundSet) {
		idx := uint64(0)
		set.IterAll(func(v Value) {
			assert.True(simpleSet[idx].Equals(v))
			idx++
		})
	}

	simpleSet := getTestNativeOrderSet()
	tr := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	set := NewTypedSet(cs, tr, simpleSet...).(compoundSet)
	doTest(simpleSet, set)

	simpleSet = getTestRefOrderSet()
	tr = MakeCompoundType(SetKind, MakeCompoundType(RefKind, MakePrimitiveType(ValueKind)))
	set = NewTypedSet(cs, tr, simpleSet...).(compoundSet)
	doTest(simpleSet, set)
}
