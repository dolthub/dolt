package types

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testSetSize = 5000

type testSet struct {
	values []Value
	less   testSetLessFn
	tr     *Type
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

func (ts testSet) toCompoundSet() compoundSet {
	return NewTypedSet(ts.tr, ts.values...).(compoundSet)
}

type testSetGenFn func(v Number) Value

func newTestSet(length int, gen testSetGenFn, less testSetLessFn, tr *Type) testSet {
	s := rand.NewSource(4242)
	used := map[int64]bool{}

	var values []Value
	for len(values) < length {
		v := s.Int63() & 0xffffff
		if _, ok := used[v]; !ok {
			values = append(values, gen(Number(v)))
			used[v] = true
		}
	}

	return testSet{values, less, MakeSetType(tr)}
}

func getTestNativeOrderSet(scale int) testSet {
	return newTestSet(int(setPattern)*scale, func(v Number) Value {
		return v
	}, func(x, y Value) bool {
		return !y.(OrderedValue).Less(x.(OrderedValue))
	}, NumberType)
}

func getTestRefValueOrderSet(scale int) testSet {
	setType := MakeSetType(NumberType)
	return newTestSet(int(setPattern)*scale, func(v Number) Value {
		return NewTypedSet(setType, v)
	}, func(x, y Value) bool {
		return !y.Ref().Less(x.Ref())
	}, setType)
}

func getTestRefToNativeOrderSet(scale int, vw ValueWriter) testSet {
	refType := MakeRefType(NumberType)
	return newTestSet(int(setPattern)*scale, func(v Number) Value {
		return vw.WriteValue(v)
	}, func(x, y Value) bool {
		return !y.(Ref).TargetRef().Less(x.(Ref).TargetRef())
	}, refType)
}

func getTestRefToValueOrderSet(scale int, vw ValueWriter) testSet {
	setType := MakeSetType(NumberType)
	refType := MakeRefType(setType)
	return newTestSet(int(setPattern)*scale, func(v Number) Value {
		return vw.WriteValue(NewTypedSet(setType, v))
	}, func(x, y Value) bool {
		return !y.(Ref).TargetRef().Less(x.(Ref).TargetRef())
	}, refType)
}

func TestCompoundSetChunks(t *testing.T) {
	assert := assert.New(t)

	vs := NewTestValueStore()
	doTest := func(ts testSet) {
		set := ts.toCompoundSet()
		set2chunks := vs.ReadValue(vs.WriteValue(set).TargetRef()).(compoundSet).Chunks()
		for i, r := range set.Chunks() {
			assert.True(r.Type().Equals(set2chunks[i].Type()), "%s != %s", r.Type().Describe(), set2chunks[i].Type().Describe())
		}
	}

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2, vs))
	doTest(getTestRefToValueOrderSet(2, vs))
}

func TestCompoundSetHas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	vs := NewTestValueStore()
	doTest := func(ts testSet) {
		set := ts.toCompoundSet()
		set2 := vs.ReadValue(vs.WriteValue(set).TargetRef()).(compoundSet)
		for _, v := range ts.values {
			assert.True(set.Has(v))
			assert.True(set2.Has(v))
		}
	}

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2, vs))
	doTest(getTestRefToValueOrderSet(2, vs))
}

func TestCompoundSetFirst(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		s := ts.toCompoundSet()
		sort.Stable(ts)
		actual := s.First()
		assert.True(ts.values[0].Equals(actual), "%v != %v", ts.values[0], actual)
	}

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderSet(2, NewTestValueStore()))
}

func TestCompoundSetIter(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		set := ts.toCompoundSet()
		sort.Sort(ts)
		idx := uint64(0)
		endAt := uint64(setPattern)

		set.Iter(func(v Value) (done bool) {
			assert.True(ts.values[idx].Equals(v))
			if idx == endAt {
				done = true
			}
			idx++
			return
		})

		assert.Equal(endAt, idx-1)
	}

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderSet(2, NewTestValueStore()))
}

func TestCompoundSetIterAll(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		set := ts.toCompoundSet()
		sort.Sort(ts)
		idx := uint64(0)

		set.IterAll(func(v Value) {
			assert.True(ts.values[idx].Equals(v))
			idx++
		})
	}

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderSet(2, NewTestValueStore()))
}

func TestCompoundSetInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	doTest := func(incr, offset int, ts testSet) {
		expected := ts.toCompoundSet()
		run := func(from, to int) {
			actual := ts.Remove(from, to).toCompoundSet().Insert(ts.values[from:to]...)
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
		}
		for i := 0; i < len(ts.values)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(ts.values)-offset, len(ts.values))
		assert.Panics(func() {
			expected.Insert(Bool(true))
		}, "Should panic due to wrong type")
	}

	doTest(18, 3, getTestNativeOrderSet(9))
	doTest(64, 1, getTestNativeOrderSet(32))
	doTest(32, 1, getTestRefValueOrderSet(4))
	doTest(32, 1, getTestRefToNativeOrderSet(4, NewTestValueStore()))
	doTest(32, 1, getTestRefToValueOrderSet(4, NewTestValueStore()))
}

func TestCompoundSetInsertExistingValue(t *testing.T) {
	assert := assert.New(t)

	ts := getTestNativeOrderSet(2)
	original := ts.toCompoundSet()
	actual := original.Insert(ts.values[0])

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestCompoundSetRemove(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	assert := assert.New(t)

	doTest := func(incr, offset int, ts testSet) {
		whole := ts.toCompoundSet()
		run := func(from, to int) {
			expected := ts.Remove(from, to).toCompoundSet()
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
	doTest(32, 1, getTestRefToNativeOrderSet(4, NewTestValueStore()))
	doTest(32, 1, getTestRefToValueOrderSet(4, NewTestValueStore()))
}

func TestCompoundSetRemoveNonexistentValue(t *testing.T) {
	assert := assert.New(t)

	ts := getTestNativeOrderSet(2)
	original := ts.toCompoundSet()
	actual := original.Remove(Number(-1)) // rand.Int63 returns non-negative values.

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestCompoundSetFilter(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		set := ts.toCompoundSet()
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
	doTest(getTestRefToNativeOrderSet(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderSet(2, NewTestValueStore()))
}

func TestCompoundSetUnion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	doTest := func(ts testSet) {
		cs := ts.toCompoundSet()
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
	doTest(getTestRefToNativeOrderSet(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderSet(2, NewTestValueStore()))
}

func TestCompoundSetFirstNNumbers(t *testing.T) {
	assert := assert.New(t)

	setType := MakeSetType(NumberType)

	nums := generateNumbersAsValues(testSetSize)
	s := newTypedSet(setType, nums...).(compoundSet)
	assert.Equal("sha1-8186877fb71711b8e6a516ed5c8ad1ccac8c6c00", s.Ref().String())
	height := deriveCompoundSetHeight(s)
	assert.Equal(height, s.tuples[0].childRef.Height())
}

func TestCompoundSetRefOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	structType, nums := generateNumbersAsRefOfStructs(testSetSize)
	refOfTypeStructType := MakeRefType(structType)
	setType := MakeSetType(refOfTypeStructType)
	s := NewTypedSet(setType, nums...).(compoundSet)
	assert.Equal("sha1-882b953455794580e6156eb21b316720aa9e45b2", s.Ref().String())
	height := deriveCompoundSetHeight(s)
	// height + 1 because the leaves are Ref values (with height 1).
	assert.Equal(height+1, s.tuples[0].childRef.Height())
}

func TestCompoundSetOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	structType, nums := generateNumbersAsStructs(testSetSize)
	setType := MakeSetType(structType)
	s := NewTypedSet(setType, nums...).(compoundSet)
	assert.Equal("sha1-f10d8ccbc2270bb52bb988a0cadff912e2723eed", s.Ref().String())
	height := deriveCompoundSetHeight(s)
	assert.Equal(height, s.tuples[0].childRef.Height())

	// has
	for _, st := range nums {
		assert.True(s.Has(st))
	}
}

func TestCompoundSetModifyAfterRead(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()
	set := getTestNativeOrderSet(2).toCompoundSet()
	// Drop chunk values.
	set = vs.ReadValue(vs.WriteValue(set).TargetRef()).(compoundSet)
	// Modify/query. Once upon a time this would crash.
	fst := set.First()
	set = set.Remove(fst).(compoundSet)
	assert.False(set.Has(fst))
	assert.True(set.Has(set.First()))
	set = set.Insert(fst).(compoundSet)
	assert.True(set.Has(fst))
}

func deriveCompoundSetHeight(s compoundSet) uint64 {
	// Note: not using mt.childRef.Height() because the purpose of this method is to be redundant.
	height := uint64(1)
	if s2, ok := s.getItem(0).(metaTuple).child.(compoundSet); ok {
		height += deriveCompoundSetHeight(s2)
	}
	return height
}
