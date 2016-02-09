package types

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
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

func (ts testSet) Remove(from, to int) testSet {
	values := make([]Value, 0, len(ts.values)-(to-from))
	values = append(values, ts.values[:from]...)
	values = append(values, ts.values[to:]...)
	return testSet{values, ts.less, ts.tr}
}

func (ts testSet) toCompoundSet(cs chunks.ChunkStore) Set {
	return NewTypedSet(ts.tr, ts.values...).(compoundSet)
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

func getTestNativeOrderSet(scale int) testSet {
	return newTestSet(int(setPattern)*scale, func(v Int64) Value {
		return v
	}, func(x, y Value) bool {
		return !y.(OrderedValue).Less(x.(OrderedValue))
	}, MakePrimitiveType(Int64Kind))
}

func getTestRefValueOrderSet(scale int) testSet {
	setType := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	return newTestSet(int(setPattern)*scale, func(v Int64) Value {
		return NewTypedSet(setType, v)
	}, func(x, y Value) bool {
		return !y.Ref().Less(x.Ref())
	}, setType)
}

func getTestRefToNativeOrderSet(scale int) testSet {
	refType := MakeCompoundType(RefKind, MakePrimitiveType(Int64Kind))
	return newTestSet(int(setPattern)*scale, func(v Int64) Value {
		return newRef(v.Ref(), refType)
	}, func(x, y Value) bool {
		return !y.(RefBase).TargetRef().Less(x.(RefBase).TargetRef())
	}, refType)
}

func getTestRefToValueOrderSet(scale int) testSet {
	setType := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	refType := MakeCompoundType(RefKind, setType)
	return newTestSet(int(setPattern)*scale, func(v Int64) Value {
		return newRef(NewTypedSet(setType, v).Ref(), refType)
	}, func(x, y Value) bool {
		return !y.(RefBase).TargetRef().Less(x.(RefBase).TargetRef())
	}, refType)
}

func TestCompoundSetHas(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		cs := chunks.NewMemoryStore()
		set := ts.toCompoundSet(cs)
		set2 := ReadValue(WriteValue(set, cs), cs).(compoundSet)
		for _, v := range ts.values {
			assert.True(set.Has(v))
			assert.True(set2.Has(v))
		}
	}

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2))
	doTest(getTestRefToValueOrderSet(2))
}

func TestCompoundSetFirst(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		s := ts.toCompoundSet(chunks.NewMemoryStore())
		sort.Stable(ts)
		actual := s.First()
		assert.True(ts.values[0].Equals(actual), "%v != %v", ts.values[0], actual)
	}

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2))
	doTest(getTestRefToValueOrderSet(2))
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

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2))
	doTest(getTestRefToValueOrderSet(2))
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

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2))
	doTest(getTestRefToValueOrderSet(2))
}

func TestCompoundSetInsert(t *testing.T) {
	assert := assert.New(t)

	doTest := func(incr, offset int, ts testSet) {
		cs := chunks.NewMemoryStore()
		expected := ts.toCompoundSet(cs)
		run := func(from, to int) {
			actual := ts.Remove(from, to).toCompoundSet(cs).Insert(ts.values[from:to]...)
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
		}
		for i := 0; i < len(ts.values)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(ts.values)-offset, len(ts.values))
		assert.Panics(func() {
			expected.Insert(Int8(1))
		}, "Should panic due to wrong type")
	}

	doTest(18, 3, getTestNativeOrderSet(9))
	doTest(64, 1, getTestNativeOrderSet(32))
	doTest(32, 1, getTestRefValueOrderSet(4))
	doTest(32, 1, getTestRefToNativeOrderSet(4))
	doTest(32, 1, getTestRefToValueOrderSet(4))
}

func TestCompoundSetInsertExistingValue(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	ts := getTestNativeOrderSet(2)
	original := ts.toCompoundSet(cs)
	actual := original.Insert(ts.values[0])

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestCompoundSetRemove(t *testing.T) {
	assert := assert.New(t)

	doTest := func(incr, offset int, ts testSet) {
		cs := chunks.NewMemoryStore()
		whole := ts.toCompoundSet(cs)
		run := func(from, to int) {
			expected := ts.Remove(from, to).toCompoundSet(cs)
			actual := whole.Remove(ts.values[from:to]...)
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
		}
		for i := 0; i < len(ts.values)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(ts.values)-offset, len(ts.values))
	}

	doTest(18, 3, getTestNativeOrderSet(9))
	doTest(64, 1, getTestNativeOrderSet(32))
	doTest(32, 1, getTestRefValueOrderSet(4))
	doTest(32, 1, getTestRefToNativeOrderSet(4))
	doTest(32, 1, getTestRefToValueOrderSet(4))
}

func TestCompoundSetRemoveNonexistentValue(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	ts := getTestNativeOrderSet(2)
	original := ts.toCompoundSet(cs)
	actual := original.Remove(Int64(-1)) // rand.Int63 returns non-negative values.

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestCompoundSetFilter(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		cs := chunks.NewMemoryStore()
		set := ts.toCompoundSet(cs)
		sort.Sort(ts)
		pivotPoint := 10
		pivot := ts.values[pivotPoint]
		actual := set.Filter(func(v Value) bool {
			return ts.less(v, pivot)
		})
		assert.True(newTypedSet(ts.tr, ts.values[:pivotPoint+1]...).Equals(actual))

		idx := 0
		actual.IterAll(func(v Value) {
			assert.True(ts.values[idx].Equals(v), "%v != %v", v, ts.values[idx])
			idx++
		})
	}

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2))
	doTest(getTestRefToValueOrderSet(2))
}

func TestCompoundSetUnion(t *testing.T) {
	assert := assert.New(t)
	ms := chunks.NewMemoryStore()

	doTest := func(ts testSet) {
		cs := ts.toCompoundSet(ms)
		cs2 := cs.Union()
		assert.True(cs.Equals(cs2))
		cs3 := cs.Union(cs2)
		assert.True(cs.Equals(cs3))
		cs4 := cs.Union(cs2, cs3)
		assert.True(cs.Equals(cs4))
		emptySet := NewTypedSet(ts.tr)
		cs5 := cs.Union(emptySet)
		assert.True(cs.Equals(cs5))
		cs6 := emptySet.Union(cs)
		assert.True(cs.Equals(cs6))

		r := rand.New(rand.NewSource(123))
		subsetValues1 := make([]Value, 0, len(ts.values))
		subsetValues2 := make([]Value, 0, len(ts.values))
		subsetValues3 := make([]Value, 0, len(ts.values))
		subsetValuesAll := make([]Value, 0, len(ts.values))
		for _, v := range ts.values {
			if r.Intn(3) == 0 {
				subsetValues1 = append(subsetValues1, v)
				subsetValuesAll = append(subsetValuesAll, v)
			} else if r.Intn(3) == 0 {
				subsetValues2 = append(subsetValues2, v)
				subsetValuesAll = append(subsetValuesAll, v)
			} else if r.Intn(3) == 0 {
				subsetValues3 = append(subsetValues3, v)
				subsetValuesAll = append(subsetValuesAll, v)
			}
		}

		s1 := NewTypedSet(ts.tr, subsetValues1...)
		s2 := NewTypedSet(ts.tr, subsetValues2...)
		s3 := NewTypedSet(ts.tr, subsetValues3...)
		sAll := NewTypedSet(ts.tr, subsetValuesAll...)

		assert.True(s1.Union(s2, s3).Equals(sAll))
	}

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2))
	doTest(getTestRefToValueOrderSet(2))
}

func TestCompoundSetFirstNNumbers(t *testing.T) {
	assert := assert.New(t)

	setType := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))

	firstNNumbers := func(n int) []Value {
		nums := []Value{}
		for i := 0; i < n; i++ {
			nums = append(nums, Int64(i))
		}

		return nums
	}

	nums := firstNNumbers(5000)
	s := newTypedSet(setType, nums...)
	assert.Equal(s.Ref().String(), "sha1-54ff8f84b5f39fe2171572922d067257a57c539c")
}
