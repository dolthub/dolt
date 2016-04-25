package types

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

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

type testSetGenFn func(v Int64) Value

func newTestSet(length int, gen testSetGenFn, less testSetLessFn, tr *Type) testSet {
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

	return testSet{values, less, MakeSetType(tr)}
}

func getTestNativeOrderSet(scale int) testSet {
	return newTestSet(int(setPattern)*scale, func(v Int64) Value {
		return v
	}, func(x, y Value) bool {
		return !y.(OrderedValue).Less(x.(OrderedValue))
	}, Int64Type)
}

func getTestRefValueOrderSet(scale int) testSet {
	setType := MakeSetType(Int64Type)
	return newTestSet(int(setPattern)*scale, func(v Int64) Value {
		return NewTypedSet(setType, v)
	}, func(x, y Value) bool {
		return !y.Ref().Less(x.Ref())
	}, setType)
}

func getTestRefToNativeOrderSet(scale int, vw ValueWriter) testSet {
	refType := MakeRefType(Int64Type)
	return newTestSet(int(setPattern)*scale, func(v Int64) Value {
		return vw.WriteValue(v)
	}, func(x, y Value) bool {
		return !y.(Ref).TargetRef().Less(x.(Ref).TargetRef())
	}, refType)
}

func getTestRefToValueOrderSet(scale int, vw ValueWriter) testSet {
	setType := MakeSetType(Int64Type)
	refType := MakeRefType(setType)
	return newTestSet(int(setPattern)*scale, func(v Int64) Value {
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
			expected.Insert(Int8(1))
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
	actual := original.Remove(Int64(-1)) // rand.Int63 returns non-negative values.

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

	setType := MakeSetType(Int64Type)

	firstNNumbers := func(n int) []Value {
		nums := []Value{}
		for i := 0; i < n; i++ {
			nums = append(nums, Int64(i))
		}

		return nums
	}

	nums := firstNNumbers(5000)
	s := newTypedSet(setType, nums...)
	assert.Equal(s.Ref().String(), "sha1-b8ce0af4afd144c64f58e393283407cc0321b0c3")
}

func TestCompoundSetRefOfStructFirstNNumbers(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	structTypeDef := MakeStructType("num", []Field{
		Field{"n", Int64Type, false},
	}, []Field{})
	pkg := NewPackage([]*Type{structTypeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	structType := MakeType(pkgRef, 0)
	refOfTypeStructType := MakeRefType(structType)

	setType := MakeSetType(refOfTypeStructType)

	firstNNumbers := func(n int) []Value {
		nums := []Value{}
		for i := 0; i < n; i++ {
			r := vs.WriteValue(NewStruct(structType, structTypeDef, structData{"n": Int64(i)}))
			nums = append(nums, r)
		}

		return nums
	}

	nums := firstNNumbers(5000)
	s := NewTypedSet(setType, nums...)
	assert.Equal("sha1-f1126a3e01f462c6dd97e49dcaa79b9a448ee162", s.Ref().String())
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
