package types

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const testSetSize = 5000

type testSetGenFn func(v Number) Value
type testSetLessFn func(x, y Value) bool

type testSet struct {
	values []Value
	less   testSetLessFn
	tr     *Type
}

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

func (ts testSet) toSet() Set {
	return NewTypedSet(ts.tr, ts.values...).(compoundSet)
}

func newTestSet(length int) testSet {
	var values []Value
	for i := 0; i < length; i++ {
		values = append(values, Number(i))
	}

	return testSet{values,
		func(x, y Value) bool {
			return !y.(OrderedValue).Less(x.(OrderedValue))
		},
		MakeSetType(NumberType)}
}

func newTestSetWithGen(length int, gen testSetGenFn, less testSetLessFn, tr *Type) testSet {
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

type setTestSuite struct {
	collectionTestSuite
	elems testSet
}

func newSetTestSuite(size uint, expectRefStr string, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int) *setTestSuite {
	length := 1 << size
	elems := newTestSet(length)
	tr := MakeSetType(NumberType)
	set := NewTypedSet(tr, elems.values...)
	return &setTestSuite{
		collectionTestSuite: collectionTestSuite{
			col:                    set,
			expectType:             tr,
			expectLen:              uint64(length),
			expectRef:              expectRefStr,
			expectChunkCount:       expectChunkCount,
			expectPrependChunkDiff: expectPrependChunkDiff,
			expectAppendChunkDiff:  expectAppendChunkDiff,
			validate: func(v2 Collection) bool {
				l2 := v2.(Set)
				out := []Value{}
				l2.IterAll(func(v Value) {
					out = append(out, v)
				})
				return valueSlicesEqual(elems.values, out)
			},
			prependOne: func() Collection {
				dup := make([]Value, length+1)
				dup[0] = Number(-1)
				copy(dup[1:], elems.values)
				return NewTypedSet(tr, dup...)
			},
			appendOne: func() Collection {
				dup := make([]Value, length+1)
				copy(dup, elems.values)
				dup[len(dup)-1] = Number(length + 1)
				return NewTypedSet(tr, dup...)
			},
		},
		elems: elems,
	}
}

func TestSetSuite1K(t *testing.T) {
	suite.Run(t, newSetTestSuite(10, "sha1-8836444230d08c68f55d936268350b6d148c4f88", 16, 2, 2))
}

func TestSetSuite4K(t *testing.T) {
	suite.Run(t, newSetTestSuite(12, "sha1-9831a1058d5ddddb269900704566e5e3697e7ac9", 3, 2, 2))
}

func getTestNativeOrderSet(scale int) testSet {
	return newTestSetWithGen(int(setPattern)*scale, func(v Number) Value {
		return v
	}, func(x, y Value) bool {
		return !y.(OrderedValue).Less(x.(OrderedValue))
	}, NumberType)
}

func getTestRefValueOrderSet(scale int) testSet {
	setType := MakeSetType(NumberType)
	return newTestSetWithGen(int(setPattern)*scale, func(v Number) Value {
		return NewTypedSet(setType, v)
	}, func(x, y Value) bool {
		return !y.Ref().Less(x.Ref())
	}, setType)
}

func getTestRefToNativeOrderSet(scale int, vw ValueWriter) testSet {
	refType := MakeRefType(NumberType)
	return newTestSetWithGen(int(setPattern)*scale, func(v Number) Value {
		return vw.WriteValue(v)
	}, func(x, y Value) bool {
		return !y.(Ref).TargetRef().Less(x.(Ref).TargetRef())
	}, refType)
}

func getTestRefToValueOrderSet(scale int, vw ValueWriter) testSet {
	setType := MakeSetType(NumberType)
	refType := MakeRefType(setType)
	return newTestSetWithGen(int(setPattern)*scale, func(v Number) Value {
		return vw.WriteValue(NewTypedSet(setType, v))
	}, func(x, y Value) bool {
		return !y.(Ref).TargetRef().Less(x.(Ref).TargetRef())
	}, refType)
}

func TestNewSet(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	assert.IsType(setType, s.Type())
	assert.Equal(uint64(0), s.Len())

	s = NewSet(Number(0))
	assert.IsType(setType, s.Type())

	s = NewTypedSet(MakeSetType(NumberType))
	assert.IsType(MakeSetType(NumberType), s.Type())

	s2 := s.Remove(Number(1))
	assert.IsType(s.Type(), s2.Type())
}

func TestSetLen(t *testing.T) {
	assert := assert.New(t)
	s0 := NewSet()
	assert.Equal(uint64(0), s0.Len())
	s1 := NewSet(Bool(true), Number(1), NewString("hi"))
	assert.Equal(uint64(3), s1.Len())
	s2 := s1.Insert(Bool(false))
	assert.Equal(uint64(4), s2.Len())
	s3 := s2.Remove(Bool(true))
	assert.Equal(uint64(3), s3.Len())
}

func TestSetEmpty(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	assert.True(s.Empty())
	assert.Equal(uint64(0), s.Len())
}

func TestSetEmptyInsert(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	assert.True(s.Empty())
	s = s.Insert(Bool(false))
	assert.False(s.Empty())
	assert.Equal(uint64(1), s.Len())
}

func TestSetEmptyInsertRemove(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	assert.True(s.Empty())
	s = s.Insert(Bool(false))
	assert.False(s.Empty())
	assert.Equal(uint64(1), s.Len())
	s = s.Remove(Bool(false))
	assert.True(s.Empty())
	assert.Equal(uint64(0), s.Len())
}

// BUG 98
func TestSetDuplicateInsert(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(Bool(true), Number(42), Number(42))
	assert.Equal(uint64(2), s1.Len())
}

func TestSetUniqueKeysString(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(NewString("hello"), NewString("world"), NewString("hello"))
	assert.Equal(uint64(2), s1.Len())
	assert.True(s1.Has(NewString("hello")))
	assert.True(s1.Has(NewString("world")))
	assert.False(s1.Has(NewString("foo")))
}

func TestSetUniqueKeysNumber(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(Number(4), Number(1), Number(0), Number(0), Number(1), Number(3))
	assert.Equal(uint64(4), s1.Len())
	assert.True(s1.Has(Number(4)))
	assert.True(s1.Has(Number(1)))
	assert.True(s1.Has(Number(0)))
	assert.True(s1.Has(Number(3)))
	assert.False(s1.Has(Number(2)))
}

func TestSetHas(t *testing.T) {
	assert := assert.New(t)
	s1 := NewSet(Bool(true), Number(1), NewString("hi"))
	assert.True(s1.Has(Bool(true)))
	assert.False(s1.Has(Bool(false)))
	assert.True(s1.Has(Number(1)))
	assert.False(s1.Has(Number(0)))
	assert.True(s1.Has(NewString("hi")))
	assert.False(s1.Has(NewString("ho")))

	s2 := s1.Insert(Bool(false))
	assert.True(s2.Has(Bool(false)))
	assert.True(s2.Has(Bool(true)))

	assert.True(s1.Has(Bool(true)))
	assert.False(s1.Has(Bool(false)))
}

func TestSetHas2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	vs := NewTestValueStore()
	doTest := func(ts testSet) {
		set := ts.toSet()
		set2 := vs.ReadValue(vs.WriteValue(set).TargetRef()).(Set)
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

func TestSetInsert(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	v1 := Bool(false)
	v2 := Bool(true)
	v3 := Number(0)

	assert.False(s.Has(v1))
	s = s.Insert(v1)
	assert.True(s.Has(v1))
	s = s.Insert(v2)
	assert.True(s.Has(v1))
	assert.True(s.Has(v2))
	s2 := s.Insert(v3)
	assert.True(s.Has(v1))
	assert.True(s.Has(v2))
	assert.False(s.Has(v3))
	assert.True(s2.Has(v1))
	assert.True(s2.Has(v2))
	assert.True(s2.Has(v3))
}

func TestSetInsert2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	doTest := func(incr, offset int, ts testSet) {
		expected := ts.toSet()
		run := func(from, to int) {
			actual := ts.Remove(from, to).toSet().Insert(ts.values[from:to]...)
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

func TestSetInsertExistingValue(t *testing.T) {
	assert := assert.New(t)

	ts := getTestNativeOrderSet(2)
	original := ts.toSet()
	actual := original.Insert(ts.values[0])

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestSetRemove(t *testing.T) {
	assert := assert.New(t)
	v1 := Bool(false)
	v2 := Bool(true)
	v3 := Number(0)
	s := NewSet(v1, v2, v3)
	assert.True(s.Has(v1))
	assert.True(s.Has(v2))
	assert.True(s.Has(v3))
	s = s.Remove(v1)
	assert.False(s.Has(v1))
	assert.True(s.Has(v2))
	assert.True(s.Has(v3))
	s2 := s.Remove(v2)
	assert.False(s.Has(v1))
	assert.True(s.Has(v2))
	assert.True(s.Has(v3))
	assert.False(s2.Has(v1))
	assert.False(s2.Has(v2))
	assert.True(s2.Has(v3))
}

func TestSetRemove2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	assert := assert.New(t)

	doTest := func(incr, offset int, ts testSet) {
		whole := ts.toSet()
		run := func(from, to int) {
			expected := ts.Remove(from, to).toSet()
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

func TestSetRemoveNonexistentValue(t *testing.T) {
	assert := assert.New(t)

	ts := getTestNativeOrderSet(2)
	original := ts.toSet()
	actual := original.Remove(Number(-1)) // rand.Int63 returns non-negative values.

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestSetFirst(t *testing.T) {
	assert := assert.New(t)
	s := NewSet()
	assert.Nil(s.First())
	s = s.Insert(Number(1))
	assert.NotNil(s.First())
	s = s.Insert(Number(2))
	assert.NotNil(s.First())
	s2 := s.Remove(Number(1))
	assert.NotNil(s2.First())
	s2 = s2.Remove(Number(2))
	assert.Nil(s2.First())
}

func TestSetOfStruct(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S1", TypeMap{
		"o": NumberType,
	})

	elems := []Value{}
	for i := 0; i < 200; i++ {
		elems = append(elems, newStructFromData(structData{"o": Number(i)}, typ))
	}

	s := NewTypedSet(MakeSetType(typ), elems...)
	for i := 0; i < 200; i++ {
		assert.True(s.Has(elems[i]))
	}
}

func TestSetIter(t *testing.T) {
	assert := assert.New(t)
	s := NewSet(Number(0), Number(1), Number(2), Number(3), Number(4))
	acc := NewSet()
	s.Iter(func(v Value) bool {
		_, ok := v.(Number)
		assert.True(ok)
		acc = acc.Insert(v)
		return false
	})
	assert.True(s.Equals(acc))

	acc = NewSet()
	s.Iter(func(v Value) bool {
		return true
	})
	assert.True(acc.Empty())
}

func TestSetIter2(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		set := ts.toSet()
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

func TestSetIterAll(t *testing.T) {
	assert := assert.New(t)
	s := NewSet(Number(0), Number(1), Number(2), Number(3), Number(4))
	acc := NewSet()
	s.IterAll(func(v Value) {
		_, ok := v.(Number)
		assert.True(ok)
		acc = acc.Insert(v)
	})
	assert.True(s.Equals(acc))
}

func TestSetIterAll2(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		set := ts.toSet()
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

func testSetOrder(assert *assert.Assertions, valueType *Type, value []Value, expectOrdering []Value) {
	mapTr := MakeSetType(valueType)
	m := NewTypedSet(mapTr, value...)
	i := 0
	m.IterAll(func(value Value) {
		assert.Equal(expectOrdering[i].Ref().String(), value.Ref().String())
		i++
	})
}

func TestSetOrdering(t *testing.T) {
	assert := assert.New(t)

	testSetOrder(assert,
		StringType,
		[]Value{
			NewString("a"),
			NewString("z"),
			NewString("b"),
			NewString("y"),
			NewString("c"),
			NewString("x"),
		},
		[]Value{
			NewString("a"),
			NewString("b"),
			NewString("c"),
			NewString("x"),
			NewString("y"),
			NewString("z"),
		},
	)

	testSetOrder(assert,
		NumberType,
		[]Value{
			Number(0),
			Number(1000),
			Number(1),
			Number(100),
			Number(2),
			Number(10),
		},
		[]Value{
			Number(0),
			Number(1),
			Number(2),
			Number(10),
			Number(100),
			Number(1000),
		},
	)

	testSetOrder(assert,
		NumberType,
		[]Value{
			Number(0),
			Number(-30),
			Number(25),
			Number(1002),
			Number(-5050),
			Number(23),
		},
		[]Value{
			Number(-5050),
			Number(-30),
			Number(0),
			Number(23),
			Number(25),
			Number(1002),
		},
	)

	testSetOrder(assert,
		NumberType,
		[]Value{
			Number(0.0001),
			Number(0.000001),
			Number(1),
			Number(25.01e3),
			Number(-32.231123e5),
			Number(23),
		},
		[]Value{
			Number(-32.231123e5),
			Number(0.000001),
			Number(0.0001),
			Number(1),
			Number(23),
			Number(25.01e3),
		},
	)

	testSetOrder(assert,
		ValueType,
		[]Value{
			NewString("a"),
			NewString("z"),
			NewString("b"),
			NewString("y"),
			NewString("c"),
			NewString("x"),
		},
		// Ordered by ref
		[]Value{
			NewString("z"),
			NewString("c"),
			NewString("a"),
			NewString("x"),
			NewString("b"),
			NewString("y"),
		},
	)

	testSetOrder(assert,
		BoolType,
		[]Value{
			Bool(true),
			Bool(false),
		},
		// Ordered by ref
		[]Value{
			Bool(true),
			Bool(false),
		},
	)
}

func TestSetFilter(t *testing.T) {
	assert := assert.New(t)

	s := NewSet(Number(0), Number(1), Number(2), Number(3), Number(4))
	s2 := s.Filter(func(v Value) bool {
		i, ok := v.(Number)
		assert.True(ok)
		return uint64(i)%2 == 0
	})

	s3 := s.Filter(func(v Value) bool {
		i, ok := v.(Number)
		assert.True(ok)
		return uint64(i)%3 == 0
	})

	assert.True(NewSet(Number(0), Number(2), Number(4)).Equals(s2))
	assert.True(NewSet(Number(0), Number(3)).Equals(s3))
}

func TestSetFilter2(t *testing.T) {
	assert := assert.New(t)

	doTest := func(ts testSet) {
		set := ts.toSet()
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

func TestSetType(t *testing.T) {
	assert := assert.New(t)

	s := NewSet()
	assert.True(s.Type().Equals(setType))

	s = NewSet(Number(0))
	assert.True(s.Type().Equals(setType))

	s = NewTypedSet(MakeSetType(NumberType))
	assert.True(s.Type().Equals(MakeSetType(NumberType)))

	s2 := s.Remove(Number(1))
	assert.True(s.Type().Equals(s2.Type()))

	s2 = s.Filter(func(v Value) bool {
		return true
	})
	assert.True(s.Type().Equals(s2.Type()))

	s2 = s.Insert(Number(0), Number(1))
	assert.True(s.Type().Equals(s2.Type()))

	assert.Panics(func() { s.Insert(Bool(true)) })
	assert.Panics(func() { s.Insert(Number(3), Bool(true)) })
}

func TestSetChunks(t *testing.T) {
	assert := assert.New(t)

	l1 := NewSet(Number(0))
	c1 := l1.Chunks()
	assert.Len(c1, 0)

	l2 := NewSet(NewTypedRefFromValue(Number(0)))
	c2 := l2.Chunks()
	assert.Len(c2, 1)
}

func TestSetChunks2(t *testing.T) {
	assert := assert.New(t)

	vs := NewTestValueStore()
	doTest := func(ts testSet) {
		set := ts.toSet()
		set2chunks := vs.ReadValue(vs.WriteValue(set).TargetRef()).Chunks()
		for i, r := range set.Chunks() {
			assert.True(r.Type().Equals(set2chunks[i].Type()), "%s != %s", r.Type().Describe(), set2chunks[i].Type().Describe())
		}
	}

	doTest(getTestNativeOrderSet(16))
	doTest(getTestRefValueOrderSet(2))
	doTest(getTestRefToNativeOrderSet(2, vs))
	doTest(getTestRefToValueOrderSet(2, vs))
}

func TestSetFirstNNumbers(t *testing.T) {
	assert := assert.New(t)

	setType := MakeSetType(NumberType)

	nums := generateNumbersAsValues(testSetSize)
	s := NewTypedSet(setType, nums...)
	assert.Equal("sha1-8186877fb71711b8e6a516ed5c8ad1ccac8c6c00", s.Ref().String())
	height := deriveSetHeight(s)
	cs := s.(compoundSet)
	assert.Equal(height, cs.tuples[0].childRef.Height())
}

func TestSetRefOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	structType, nums := generateNumbersAsRefOfStructs(testSetSize)
	refOfTypeStructType := MakeRefType(structType)
	setType := MakeSetType(refOfTypeStructType)
	s := NewTypedSet(setType, nums...)
	assert.Equal("sha1-882b953455794580e6156eb21b316720aa9e45b2", s.Ref().String())
	height := deriveSetHeight(s)
	cs := s.(compoundSet)
	// height + 1 because the leaves are Ref values (with height 1).
	assert.Equal(height+1, cs.tuples[0].childRef.Height())
}

func TestSetModifyAfterRead(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()
	set := getTestNativeOrderSet(2).toSet()
	// Drop chunk values.
	set = vs.ReadValue(vs.WriteValue(set).TargetRef()).(Set)
	// Modify/query. Once upon a time this would crash.
	fst := set.First()
	set = set.Remove(fst)
	assert.False(set.Has(fst))
	assert.True(set.Has(set.First()))
	set = set.Insert(fst)
	assert.True(set.Has(fst))
}

func deriveSetHeight(s Set) uint64 {
	// Note: not using mt.childRef.Height() because the purpose of this method is to be redundant.
	height := uint64(1)
	cs := s.(compoundSet)
	if s2, ok := cs.getItem(0).(metaTuple).child.(compoundSet); ok {
		height += deriveSetHeight(s2)
	}
	return height
}
