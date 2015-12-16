package types

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

type testSet struct {
	values []Value
	less   testSetLessFn
	tr     Type
}

type testSetLessFn func(x, y Value) bool

func (ts testSet) Len() int {
	return len(ts.values)
}

func (ts testSet) Less(i, j int) bool {
	return ts.less(ts.values[i], ts.values[j])
}

func (ts testSet) Swap(i, j int) {
	ts.values[i], ts.values[j] = ts.values[j], ts.values[i]
}

func (ts testSet) toCompoundSet(cs chunks.ChunkStore) compoundSet {
	return NewTypedSet(cs, ts.tr, ts.values...).(compoundSet)
}

type testSetGenFn func(v Int64) Value

func newTestSet(length int, gen testSetGenFn, less testSetLessFn, tr Type) testSet {
	s := rand.NewSource(4242)
	used := map[int64]bool{}

	var values []Value
	for len(values) < length {
		v := s.Int63() & 0xffffff
		if _, ok := used[v]; !ok {
			values = append(values, gen(Int64(v)))
			used[v] = true
		}
	}

	return testSet{values, less, MakeCompoundType(SetKind, tr)}
}

func getTestNativeOrderSet() testSet {
	return newTestSet(int(setPattern*16), func(v Int64) Value {
		return v
	}, func(x, y Value) bool {
		return !y.(OrderedValue).Less(x.(OrderedValue))
	}, MakePrimitiveType(Int64Kind))
}

func getTestRefValueOrderSet() testSet {
	setType := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	return newTestSet(int(setPattern*2), func(v Int64) Value {
		return NewTypedSet(chunks.NewMemoryStore(), setType, v)
	}, func(x, y Value) bool {
		return !y.Ref().Less(x.Ref())
	}, setType)
}

func getTestRefToNativeOrderSet() testSet {
	refType := MakeCompoundType(RefKind, MakePrimitiveType(Int64Kind))
	return newTestSet(int(setPattern*2), func(v Int64) Value {
		return newRef(v.Ref(), refType)
	}, func(x, y Value) bool {
		return !y.(RefBase).TargetRef().Less(x.(RefBase).TargetRef())
	}, refType)
}

func getTestRefToValueOrderSet() testSet {
	setType := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	refType := MakeCompoundType(RefKind, setType)
	return newTestSet(int(setPattern*2), func(v Int64) Value {
		return newRef(NewTypedSet(chunks.NewMemoryStore(), setType, v).Ref(), refType)
	}, func(x, y Value) bool {
		return !y.(RefBase).TargetRef().Less(x.(RefBase).TargetRef())
	}, refType)
}

func TestCompoundSetHas(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		set := ts.toCompoundSet(chunks.NewMemoryStore())
		for _, v := range ts.values {
			assert.True(set.Has(v))
		}
	}

	doTest(getTestNativeOrderSet())
	doTest(getTestRefValueOrderSet())
	doTest(getTestRefToNativeOrderSet())
	doTest(getTestRefToValueOrderSet())
}

func TestCompoundSetFirst(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		s := ts.toCompoundSet(chunks.NewMemoryStore())
		sort.Stable(ts)
		actual := s.First()
		assert.True(ts.values[0].Equals(actual), "%v != %v", ts.values[0], actual)
	}

	doTest(getTestNativeOrderSet())
	doTest(getTestRefValueOrderSet())
	doTest(getTestRefToNativeOrderSet())
	doTest(getTestRefToValueOrderSet())
}

func TestCompoundSetIter(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		set := ts.toCompoundSet(chunks.NewMemoryStore())
		sort.Sort(ts)
		idx := uint64(0)
		endAt := uint64(setPattern)

		set.Iter(func(v Value) bool {
			assert.True(ts.values[idx].Equals(v))
			if idx == endAt {
				idx += 1
				return true
			}

			idx += 1
			return false
		})

		assert.Equal(endAt, idx-1)
	}

	doTest(getTestNativeOrderSet())
	doTest(getTestRefValueOrderSet())
	doTest(getTestRefToNativeOrderSet())
	doTest(getTestRefToValueOrderSet())
}

func TestCompoundSetIterAll(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		set := ts.toCompoundSet(chunks.NewMemoryStore())
		sort.Sort(ts)
		idx := uint64(0)

		set.IterAll(func(v Value) {
			assert.True(ts.values[idx].Equals(v))
			idx++
		})
	}

	doTest(getTestNativeOrderSet())
	doTest(getTestRefValueOrderSet())
	doTest(getTestRefToNativeOrderSet())
	doTest(getTestRefToValueOrderSet())
}

func TestCompoundSetFilter(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		set := ts.toCompoundSet(chunks.NewMemoryStore())
		sort.Sort(ts)
		pivot := ts.values[10]
		actual := set.Filter(func(v Value) bool {
			return ts.less(v, pivot)
		})

		idx := 0
		actual.IterAll(func(v Value) {
			assert.True(ts.values[idx].Equals(v), "%v != %v", v, ts.values[idx])
			idx++
		})
	}

	doTest(getTestNativeOrderSet())
	doTest(getTestRefValueOrderSet())
	doTest(getTestRefToNativeOrderSet())
	doTest(getTestRefToValueOrderSet())
}
