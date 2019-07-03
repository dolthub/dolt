// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const testSetSize = 5000

type testSet ValueSlice

type toTestSetFunc func(scale int, vrw ValueReadWriter) testSet

func (ts testSet) Remove(from, to int) testSet {
	values := make(testSet, 0, len(ts)-(to-from))
	values = append(values, ts[:from]...)
	values = append(values, ts[to:]...)
	return values
}

func (ts testSet) Has(key Value) bool {
	for _, entry := range ts {
		if entry.Equals(key) {
			return true
		}
	}
	return false
}

func (ts testSet) Diff(last testSet) (added []Value, removed []Value) {
	// Note: this could be use ts.toSet/last.toSet and then tsSet.Diff(lastSet) but the
	// purpose of this method is to be redundant.
	if len(ts) == 0 && len(last) == 0 {
		return // nothing changed
	}
	if len(ts) == 0 {
		// everything removed
		for _, entry := range last {
			removed = append(removed, entry)
		}
		return
	}
	if len(last) == 0 {
		// everything added
		for _, entry := range ts {
			added = append(added, entry)
		}
		return
	}
	for _, entry := range ts {
		if !last.Has(entry) {
			added = append(added, entry)
		}
	}
	for _, entry := range last {
		if !ts.Has(entry) {
			removed = append(removed, entry)
		}
	}
	return
}

func (ts testSet) toSet(vrw ValueReadWriter) Set {
	return NewSet(context.Background(), vrw, ts...)
}

func newSortedTestSet(length int, gen genValueFn) (values testSet) {
	for i := 0; i < length; i++ {
		values = append(values, gen(i))
	}
	return
}

func newTestSetFromSet(s Set) testSet {
	values := make([]Value, 0, s.Len())
	s.IterAll(context.Background(), func(v Value) {
		values = append(values, v)
	})
	return values
}

func newRandomTestSet(length int, gen genValueFn) testSet {
	s := rand.NewSource(4242)
	used := map[int]bool{}

	var values []Value
	for len(values) < length {
		v := int(s.Int63()) & 0xffffff
		if _, ok := used[v]; !ok {
			values = append(values, gen(v))
			used[v] = true
		}
	}

	return values
}

func validateSet(t *testing.T, vrw ValueReadWriter, s Set, values ValueSlice) {
	assert.True(t, s.Equals(NewSet(context.Background(), vrw, values...)))
	out := ValueSlice{}
	s.IterAll(context.Background(), func(v Value) {
		out = append(out, v)
	})
	assert.True(t, out.Equals(values))
}

type setTestSuite struct {
	collectionTestSuite
	elems testSet
}

func newSetTestSuite(size uint, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int, gen genValueFn) *setTestSuite {
	vs := newTestValueStore()

	length := 1 << size
	elemType := TypeOf(gen(0))
	elems := newSortedTestSet(length, gen)
	tr := MakeSetType(elemType)
	set := NewSet(context.Background(), vs, elems...)
	return &setTestSuite{
		collectionTestSuite: collectionTestSuite{
			col:                    set,
			expectType:             tr,
			expectLen:              uint64(length),
			expectChunkCount:       expectChunkCount,
			expectPrependChunkDiff: expectPrependChunkDiff,
			expectAppendChunkDiff:  expectAppendChunkDiff,
			validate: func(v2 Collection) bool {
				l2 := v2.(Set)
				out := ValueSlice{}
				l2.IterAll(context.Background(), func(v Value) {
					out = append(out, v)
				})
				exp := ValueSlice(elems)
				rv := exp.Equals(out)
				if !rv {
					printBadCollections(exp, out)
				}
				return rv
			},
			prependOne: func() Collection {
				dup := make([]Value, length+1)
				dup[0] = Float(-1)
				copy(dup[1:], elems)
				return NewSet(context.Background(), vs, dup...)
			},
			appendOne: func() Collection {
				dup := make([]Value, length+1)
				copy(dup, elems)
				dup[len(dup)-1] = Float(length + 1)
				return NewSet(context.Background(), vs, dup...)
			},
		},
		elems: elems,
	}
}

var mutex sync.Mutex

func printBadCollections(expected, actual ValueSlice) {
	mutex.Lock()
	defer mutex.Unlock()
	fmt.Println("expected:", expected)
	fmt.Println("actual:", actual)
}

func (suite *setTestSuite) createStreamingSet(vs *ValueStore) Set {
	vChan := make(chan Value)
	setChan := NewStreamingSet(context.Background(), vs, vChan)
	for _, entry := range suite.elems {
		vChan <- entry
	}
	close(vChan)
	return <-setChan
}

func (suite *setTestSuite) TestStreamingSet() {
	vs := newTestValueStore()
	defer vs.Close()
	s := suite.createStreamingSet(vs)
	suite.True(suite.validate(s))
}

func (suite *setTestSuite) TestStreamingSetOrder() {
	vs := newTestValueStore()
	defer vs.Close()

	elems := make(testSet, len(suite.elems))
	copy(elems, suite.elems)
	elems[0], elems[1] = elems[1], elems[0]
	vChan := make(chan Value, len(elems))
	for _, e := range elems {
		vChan <- e
	}
	close(vChan)

	readInput := func(vrw ValueReadWriter, vChan <-chan Value, outChan chan<- Set) {
		readSetInput(context.Background(), Format_7_18, vrw, vChan, outChan)
	}

	testFunc := func() {
		outChan := newStreamingSet(vs, vChan, readInput)
		<-outChan
	}
	suite.Panics(testFunc)
}

func (suite *setTestSuite) TestStreamingSet2() {
	vs := newTestValueStore()
	defer vs.Close()
	wg := sync.WaitGroup{}
	wg.Add(2)
	var s1, s2 Set
	go func() {
		s1 = suite.createStreamingSet(vs)
		wg.Done()
	}()
	go func() {
		s2 = suite.createStreamingSet(vs)
		wg.Done()
	}()
	wg.Wait()
	suite.True(suite.validate(s1))
	suite.True(suite.validate(s2))
}

func TestSetSuite4K(t *testing.T) {
	suite.Run(t, newSetTestSuite(12, 8, 2, 2, newNumber))
}

func TestSetSuite4KStructs(t *testing.T) {
	suite.Run(t, newSetTestSuite(12, 9, 2, 2, newNumberStruct))
}

func getTestNativeOrderSet(scale int, vrw ValueReadWriter) testSet {
	return newRandomTestSet(64*scale, newNumber)
}

func getTestRefValueOrderSet(scale int, vrw ValueReadWriter) testSet {
	return newRandomTestSet(64*scale, newNumber)
}

func getTestRefToNativeOrderSet(scale int, vrw ValueReadWriter) testSet {
	return newRandomTestSet(64*scale, func(v int) Value {
		return vrw.WriteValue(context.Background(), Float(v))
	})
}

func getTestRefToValueOrderSet(scale int, vrw ValueReadWriter) testSet {
	return newRandomTestSet(64*scale, func(v int) Value {
		return vrw.WriteValue(context.Background(), NewSet(context.Background(), vrw, Float(v)))
	})
}

func accumulateSetDiffChanges(ctx context.Context, s1, s2 Set) (added []Value, removed []Value) {
	changes := make(chan ValueChanged)
	go func() {
		s1.Diff(ctx, s2, changes, nil)
		close(changes)
	}()
	for change := range changes {
		if change.ChangeType == DiffChangeAdded {
			added = append(added, change.Key)
		} else if change.ChangeType == DiffChangeRemoved {
			removed = append(removed, change.Key)
		}
	}
	return
}

func diffSetTest(assert *assert.Assertions, s1 Set, s2 Set, numAddsExpected int, numRemovesExpected int) (added []Value, removed []Value) {
	added, removed = accumulateSetDiffChanges(context.Background(), s1, s2)
	assert.Equal(numAddsExpected, len(added), "num added is not as expected")
	assert.Equal(numRemovesExpected, len(removed), "num removed is not as expected")

	ts1 := newTestSetFromSet(s1)
	ts2 := newTestSetFromSet(s2)
	tsAdded, tsRemoved := ts1.Diff(ts2)
	assert.Equal(numAddsExpected, len(tsAdded), "num added is not as expected")
	assert.Equal(numRemovesExpected, len(tsRemoved), "num removed is not as expected")

	assert.Equal(added, tsAdded, "set added != tsSet added")
	assert.Equal(removed, tsRemoved, "set removed != tsSet removed")
	return
}

func TestNewSet(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	s := NewSet(context.Background(), vs)
	assert.True(MakeSetType(MakeUnionType()).Equals(TypeOf(s)))
	assert.Equal(uint64(0), s.Len())

	s = NewSet(context.Background(), vs, Float(0))
	assert.True(MakeSetType(FloaTType).Equals(TypeOf(s)))

	s = NewSet(context.Background(), vs)
	assert.IsType(MakeSetType(FloaTType), TypeOf(s))

	s2 := s.Edit().Remove(Float(1)).Set(context.Background())
	assert.IsType(TypeOf(s), TypeOf(s2))
}

func TestSetLen(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	s0 := NewSet(context.Background(), vs)
	assert.Equal(uint64(0), s0.Len())
	s1 := NewSet(context.Background(), vs, Bool(true), Float(1), String("hi"))
	assert.Equal(uint64(3), s1.Len())
	diffSetTest(assert, s0, s1, 0, 3)
	diffSetTest(assert, s1, s0, 3, 0)

	s2 := s1.Edit().Insert(Bool(false)).Set(context.Background())
	assert.Equal(uint64(4), s2.Len())
	diffSetTest(assert, s0, s2, 0, 4)
	diffSetTest(assert, s2, s0, 4, 0)
	diffSetTest(assert, s1, s2, 0, 1)
	diffSetTest(assert, s2, s1, 1, 0)

	s3 := s2.Edit().Remove(Bool(true)).Set(context.Background())
	assert.Equal(uint64(3), s3.Len())
	diffSetTest(assert, s2, s3, 1, 0)
	diffSetTest(assert, s3, s2, 0, 1)
}

func TestSetEmpty(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s := NewSet(context.Background(), vs)
	assert.True(s.Empty())
	assert.Equal(uint64(0), s.Len())
}

func TestSetEmptyInsert(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s := NewSet(context.Background(), vs)
	assert.True(s.Empty())
	s = s.Edit().Insert(Bool(false)).Set(context.Background())
	assert.False(s.Empty())
	assert.Equal(uint64(1), s.Len())
}

func TestSetEmptyInsertRemove(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s := NewSet(context.Background(), vs)
	assert.True(s.Empty())
	s = s.Edit().Insert(Bool(false)).Set(context.Background())
	assert.False(s.Empty())
	assert.Equal(uint64(1), s.Len())
	s = s.Edit().Remove(Bool(false)).Set(context.Background())
	assert.True(s.Empty())
	assert.Equal(uint64(0), s.Len())
}

// BUG 98
func TestSetDuplicateInsert(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s1 := NewSet(context.Background(), vs, Bool(true), Float(42), Float(42))
	assert.Equal(uint64(2), s1.Len())
}

func TestSetUniqueKeysString(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s1 := NewSet(context.Background(), vs, String("hello"), String("world"), String("hello"))
	assert.Equal(uint64(2), s1.Len())
	assert.True(s1.Has(context.Background(), String("hello")))
	assert.True(s1.Has(context.Background(), String("world")))
	assert.False(s1.Has(context.Background(), String("foo")))
}

func TestSetUniqueKeysNumber(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s1 := NewSet(context.Background(), vs, Float(4), Float(1), Float(0), Float(0), Float(1), Float(3))
	assert.Equal(uint64(4), s1.Len())
	assert.True(s1.Has(context.Background(), Float(4)))
	assert.True(s1.Has(context.Background(), Float(1)))
	assert.True(s1.Has(context.Background(), Float(0)))
	assert.True(s1.Has(context.Background(), Float(3)))
	assert.False(s1.Has(context.Background(), Float(2)))
}

func TestSetHas(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s1 := NewSet(context.Background(), vs, Bool(true), Float(1), String("hi"))
	assert.True(s1.Has(context.Background(), Bool(true)))
	assert.False(s1.Has(context.Background(), Bool(false)))
	assert.True(s1.Has(context.Background(), Float(1)))
	assert.False(s1.Has(context.Background(), Float(0)))
	assert.True(s1.Has(context.Background(), String("hi")))
	assert.False(s1.Has(context.Background(), String("ho")))

	s2 := s1.Edit().Insert(Bool(false)).Set(context.Background())
	assert.True(s2.Has(context.Background(), Bool(false)))
	assert.True(s2.Has(context.Background(), Bool(true)))

	assert.True(s1.Has(context.Background(), Bool(true)))
	assert.False(s1.Has(context.Background(), Bool(false)))
}

func TestSetHas2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(toTestSet toTestSetFunc, scale int) {
		vrw := newTestValueStore()
		ts := toTestSet(scale, vrw)
		set := ts.toSet(vrw)
		set2 := vrw.ReadValue(context.Background(), vrw.WriteValue(context.Background(), set).TargetHash()).(Set)
		for _, v := range ts {
			assert.True(set.Has(context.Background(), v))
			assert.True(set2.Has(context.Background(), v))
		}
		diffSetTest(assert, set, set2, 0, 0)
	}

	doTest(getTestNativeOrderSet, 16)
	doTest(getTestRefValueOrderSet, 2)
	doTest(getTestRefToNativeOrderSet, 2)
	doTest(getTestRefToValueOrderSet, 2)
}

func validateSetInsertion(t *testing.T, vrw ValueReadWriter, values ValueSlice) {
	s := NewSet(context.Background(), vrw)
	for i, v := range values {
		s = s.Edit().Insert(v).Set(context.Background())
		validateSet(t, vrw, s, values[0:i+1])
	}
}

func TestSetValidateInsertAscending(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()

	validateSetInsertion(t, vs, generateNumbersAsValues(300))
}

func TestSetInsert(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s := NewSet(context.Background(), vs)
	v1 := Bool(false)
	v2 := Bool(true)
	v3 := Float(0)

	assert.False(s.Has(context.Background(), v1))
	s = s.Edit().Insert(v1).Set(context.Background())
	assert.True(s.Has(context.Background(), v1))
	s = s.Edit().Insert(v2).Set(context.Background())
	assert.True(s.Has(context.Background(), v1))
	assert.True(s.Has(context.Background(), v2))
	s2 := s.Edit().Insert(v3).Set(context.Background())
	assert.True(s.Has(context.Background(), v1))
	assert.True(s.Has(context.Background(), v2))
	assert.False(s.Has(context.Background(), v3))
	assert.True(s2.Has(context.Background(), v1))
	assert.True(s2.Has(context.Background(), v2))
	assert.True(s2.Has(context.Background(), v3))
}

func TestSetInsert2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(incr, offset int, toTestSet toTestSetFunc, scale int) {
		vrw := newTestValueStore()
		ts := toTestSet(scale, vrw)
		expected := ts.toSet(vrw)
		run := func(from, to int) {
			actual := ts.Remove(from, to).toSet(vrw).Edit().Insert(ts[from:to]...).Set(context.Background())
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
			diffSetTest(assert, expected, actual, 0, 0)
		}
		for i := 0; i < len(ts)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(ts)-offset, len(ts))
	}

	doTest(18, 3, getTestNativeOrderSet, 9)
	doTest(64, 1, getTestNativeOrderSet, 32)
	doTest(32, 1, getTestRefValueOrderSet, 4)
	doTest(32, 1, getTestRefToNativeOrderSet, 4)
	doTest(32, 1, getTestRefToValueOrderSet, 4)
}

func TestSetInsertExistingValue(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()

	ts := getTestNativeOrderSet(2, vs)
	original := ts.toSet(vs)
	actual := original.Edit().Insert(ts[0]).Set(context.Background())

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestSetRemove(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	v1 := Bool(false)
	v2 := Bool(true)
	v3 := Float(0)
	s := NewSet(context.Background(), vs, v1, v2, v3)
	assert.True(s.Has(context.Background(), v1))
	assert.True(s.Has(context.Background(), v2))
	assert.True(s.Has(context.Background(), v3))
	s = s.Edit().Remove(v1).Set(context.Background())
	assert.False(s.Has(context.Background(), v1))
	assert.True(s.Has(context.Background(), v2))
	assert.True(s.Has(context.Background(), v3))
	s2 := s.Edit().Remove(v2).Set(context.Background())
	assert.False(s.Has(context.Background(), v1))
	assert.True(s.Has(context.Background(), v2))
	assert.True(s.Has(context.Background(), v3))
	assert.False(s2.Has(context.Background(), v1))
	assert.False(s2.Has(context.Background(), v2))
	assert.True(s2.Has(context.Background(), v3))
}

func TestSetRemove2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(incr, offset int, toTestSet toTestSetFunc, scale int) {
		vrw := newTestValueStore()
		ts := toTestSet(scale, vrw)
		whole := ts.toSet(vrw)
		run := func(from, to int) {
			expected := ts.Remove(from, to).toSet(vrw)
			actual := whole.Edit().Remove(ts[from:to]...).Set(context.Background())
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
			diffSetTest(assert, expected, actual, 0, 0)
		}
		for i := 0; i < len(ts)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(ts)-offset, len(ts))
	}

	doTest(18, 3, getTestNativeOrderSet, 9)
	doTest(64, 1, getTestNativeOrderSet, 32)
	doTest(32, 1, getTestRefValueOrderSet, 4)
	doTest(32, 1, getTestRefToNativeOrderSet, 4)
	doTest(32, 1, getTestRefToValueOrderSet, 4)
}

func TestSetRemoveNonexistentValue(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	ts := getTestNativeOrderSet(2, vs)
	original := ts.toSet(vs)
	actual := original.Edit().Remove(Float(-1)).Set(context.Background()) // rand.Int63 returns non-negative values.

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestSetFirst(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s := NewSet(context.Background(), vs)
	assert.Nil(s.First(context.Background()))
	s = s.Edit().Insert(Float(1)).Set(context.Background())
	assert.NotNil(s.First(context.Background()))
	s = s.Edit().Insert(Float(2)).Set(context.Background())
	assert.NotNil(s.First(context.Background()))
	s2 := s.Edit().Remove(Float(1)).Set(context.Background())
	assert.NotNil(s2.First(context.Background()))
	s2 = s2.Edit().Remove(Float(2)).Set(context.Background())
	assert.Nil(s2.First(context.Background()))
}

func TestSetOfStruct(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	elems := []Value{}
	for i := 0; i < 200; i++ {
		elems = append(elems, NewStruct(Format_7_18, "S1", StructData{"o": Float(i)}))
	}

	s := NewSet(context.Background(), vs, elems...)
	for i := 0; i < 200; i++ {
		assert.True(s.Has(context.Background(), elems[i]))
	}
}

func TestSetIter(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s := NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3), Float(4))
	acc := NewSet(context.Background(), vs)
	s.Iter(context.Background(), func(v Value) bool {
		_, ok := v.(Float)
		assert.True(ok)
		acc = acc.Edit().Insert(v).Set(context.Background())
		return false
	})
	assert.True(s.Equals(acc))

	acc = NewSet(context.Background(), vs)
	s.Iter(context.Background(), func(v Value) bool {
		return true
	})
	assert.True(acc.Empty())
}

func TestSetIter2(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(toTestSet toTestSetFunc, scale int) {
		vrw := newTestValueStore()
		ts := toTestSet(scale, vrw)
		set := ts.toSet(vrw)
		sort.Sort(ValueSort{ts, Format_7_18})
		idx := uint64(0)
		endAt := uint64(64)

		set.Iter(context.Background(), func(v Value) (done bool) {
			assert.True(ts[idx].Equals(v))
			if idx == endAt {
				done = true
			}
			idx++
			return
		})

		assert.Equal(endAt, idx-1)
	}

	doTest(getTestNativeOrderSet, 16)
	doTest(getTestRefValueOrderSet, 2)
	doTest(getTestRefToNativeOrderSet, 2)
	doTest(getTestRefToValueOrderSet, 2)
}

func TestSetIterAll(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s := NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3), Float(4))
	acc := NewSet(context.Background(), vs)
	s.IterAll(context.Background(), func(v Value) {
		_, ok := v.(Float)
		assert.True(ok)
		acc = acc.Edit().Insert(v).Set(context.Background())
	})
	assert.True(s.Equals(acc))
}

func TestSetIterAll2(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(toTestSet toTestSetFunc, scale int) {
		vrw := newTestValueStore()
		ts := toTestSet(scale, vrw)
		set := ts.toSet(vrw)
		sort.Sort(ValueSort{ts, Format_7_18})
		idx := uint64(0)

		set.IterAll(context.Background(), func(v Value) {
			assert.True(ts[idx].Equals(v))
			idx++
		})
	}

	doTest(getTestNativeOrderSet, 16)
	doTest(getTestRefValueOrderSet, 2)
	doTest(getTestRefToNativeOrderSet, 2)
	doTest(getTestRefToValueOrderSet, 2)
}

func testSetOrder(assert *assert.Assertions, vrw ValueReadWriter, valueType *Type, value []Value, expectOrdering []Value) {
	m := NewSet(context.Background(), vrw, value...)
	i := 0
	m.IterAll(context.Background(), func(value Value) {
		// TODO(binformat)
		assert.Equal(expectOrdering[i].Hash(Format_7_18).String(), value.Hash(Format_7_18).String())
		i++
	})
}

func TestSetOrdering(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	testSetOrder(assert, vs,
		StringType,
		[]Value{
			String("a"),
			String("z"),
			String("b"),
			String("y"),
			String("c"),
			String("x"),
		},
		[]Value{
			String("a"),
			String("b"),
			String("c"),
			String("x"),
			String("y"),
			String("z"),
		},
	)

	testSetOrder(assert, vs,
		FloaTType,
		[]Value{
			Float(0),
			Float(1000),
			Float(1),
			Float(100),
			Float(2),
			Float(10),
		},
		[]Value{
			Float(0),
			Float(1),
			Float(2),
			Float(10),
			Float(100),
			Float(1000),
		},
	)

	testSetOrder(assert, vs,
		FloaTType,
		[]Value{
			Float(0),
			Float(-30),
			Float(25),
			Float(1002),
			Float(-5050),
			Float(23),
		},
		[]Value{
			Float(-5050),
			Float(-30),
			Float(0),
			Float(23),
			Float(25),
			Float(1002),
		},
	)

	testSetOrder(assert, vs,
		FloaTType,
		[]Value{
			Float(0.0001),
			Float(0.000001),
			Float(1),
			Float(25.01e3),
			Float(-32.231123e5),
			Float(23),
		},
		[]Value{
			Float(-32.231123e5),
			Float(0.000001),
			Float(0.0001),
			Float(1),
			Float(23),
			Float(25.01e3),
		},
	)

	testSetOrder(assert, vs,
		ValueType,
		[]Value{
			String("a"),
			String("z"),
			String("b"),
			String("y"),
			String("c"),
			String("x"),
		},
		// Ordered by value
		[]Value{
			String("a"),
			String("b"),
			String("c"),
			String("x"),
			String("y"),
			String("z"),
		},
	)

	testSetOrder(assert, vs,
		BoolType,
		[]Value{
			Bool(true),
			Bool(false),
		},
		// Ordered by value
		[]Value{
			Bool(false),
			Bool(true),
		},
	)
}

func TestSetType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	s := NewSet(context.Background(), vs)
	assert.True(TypeOf(s).Equals(MakeSetType(MakeUnionType())))

	s = NewSet(context.Background(), vs, Float(0))
	assert.True(TypeOf(s).Equals(MakeSetType(FloaTType)))

	s2 := s.Edit().Remove(Float(1)).Set(context.Background())
	assert.True(TypeOf(s2).Equals(MakeSetType(FloaTType)))

	s2 = s.Edit().Insert(Float(0), Float(1)).Set(context.Background())
	assert.True(TypeOf(s).Equals(TypeOf(s2)))

	s3 := s.Edit().Insert(Bool(true)).Set(context.Background())
	assert.True(TypeOf(s3).Equals(MakeSetType(MakeUnionType(BoolType, FloaTType))))
	s4 := s.Edit().Insert(Float(3), Bool(true)).Set(context.Background())
	assert.True(TypeOf(s4).Equals(MakeSetType(MakeUnionType(BoolType, FloaTType))))
}

func TestSetChunks(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	l1 := NewSet(context.Background(), vs, Float(0))
	c1 := getChunks(l1)
	assert.Len(c1, 0)

	l2 := NewSet(context.Background(), vs, NewRef(Float(0), Format_7_18))
	c2 := getChunks(l2)
	assert.Len(c2, 1)
}

func TestSetChunks2(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(toTestSet toTestSetFunc, scale int) {
		vrw := newTestValueStore()
		ts := toTestSet(scale, vrw)
		set := ts.toSet(vrw)
		set2chunks := getChunks(vrw.ReadValue(context.Background(), vrw.WriteValue(context.Background(), set).TargetHash()))
		for i, r := range getChunks(set) {
			assert.True(TypeOf(r).Equals(TypeOf(set2chunks[i])), "%s != %s", TypeOf(r).Describe(context.Background()), TypeOf(set2chunks[i]).Describe(context.Background()))
		}
	}

	doTest(getTestNativeOrderSet, 16)
	doTest(getTestRefValueOrderSet, 2)
	doTest(getTestRefToNativeOrderSet, 2)
	doTest(getTestRefToValueOrderSet, 2)
}

func TestSetFirstNNumbers(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	nums := generateNumbersAsValues(testSetSize)
	s := NewSet(context.Background(), vs, nums...)
	assert.Equal(deriveCollectionHeight(s), getRefHeightOfCollection(s))
}

func TestSetRefOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)
	vs := newTestValueStore()

	nums := generateNumbersAsRefOfStructs(vs, testSetSize)
	s := NewSet(context.Background(), vs, nums...)
	// height + 1 because the leaves are Ref values (with height 1).
	assert.Equal(deriveCollectionHeight(s)+1, getRefHeightOfCollection(s))
}

func TestSetModifyAfterRead(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)
	vs := newTestValueStore()
	set := getTestNativeOrderSet(2, vs).toSet(vs)
	// Drop chunk values.
	set = vs.ReadValue(context.Background(), vs.WriteValue(context.Background(), set).TargetHash()).(Set)
	// Modify/query. Once upon a time this would crash.
	fst := set.First(context.Background())
	set = set.Edit().Remove(fst).Set(context.Background())
	assert.False(set.Has(context.Background(), fst))
	assert.True(set.Has(context.Background(), set.First(context.Background())))
	set = set.Edit().Insert(fst).Set(context.Background())
	assert.True(set.Has(context.Background(), fst))
}

func TestSetTypeAfterMutations(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	test := func(n int, c interface{}) {
		vs := newTestValueStore()
		values := generateNumbersAsValues(n)

		s := NewSet(context.Background(), vs, values...)
		assert.Equal(s.Len(), uint64(n))
		assert.IsType(c, s.asSequence())
		assert.True(TypeOf(s).Equals(MakeSetType(FloaTType)))

		s = s.Edit().Insert(String("a")).Set(context.Background())
		assert.Equal(s.Len(), uint64(n+1))
		assert.IsType(c, s.asSequence())
		assert.True(TypeOf(s).Equals(MakeSetType(MakeUnionType(FloaTType, StringType))))

		s = s.Edit().Remove(String("a")).Set(context.Background())
		assert.Equal(s.Len(), uint64(n))
		assert.IsType(c, s.asSequence())
		assert.True(TypeOf(s).Equals(MakeSetType(FloaTType)))
	}

	test(10, setLeafSequence{})
	test(2000, metaSequence{})
}

func TestChunkedSetWithValuesOfEveryType(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	smallTestChunks()
	defer normalProductionChunks()

	vals := []Value{
		// Values
		Bool(true),
		Float(0),
		String("hello"),
		NewBlob(context.Background(), vs, bytes.NewBufferString("buf")),
		NewSet(context.Background(), vs, Bool(true)),
		NewList(context.Background(), vs, Bool(true)),
		NewMap(context.Background(), vs, Bool(true), Float(0)),
		NewStruct(Format_7_18, "", StructData{"field": Bool(true)}),
		// Refs of values
		NewRef(Bool(true), Format_7_18),
		NewRef(Float(0), Format_7_18),
		NewRef(String("hello"), Format_7_18),
		NewRef(NewBlob(context.Background(), vs, bytes.NewBufferString("buf")), Format_7_18),
		NewRef(NewSet(context.Background(), vs, Bool(true)), Format_7_18),
		NewRef(NewList(context.Background(), vs, Bool(true)), Format_7_18),
		NewRef(NewMap(context.Background(), vs, Bool(true), Float(0)), Format_7_18),
		NewRef(NewStruct(Format_7_18, "", StructData{"field": Bool(true)}), Format_7_18),
	}

	s := NewSet(context.Background(), vs, vals...)
	for i := 1; s.asSequence().isLeaf(); i++ {
		v := Float(i)
		vals = append(vals, v)
		s = s.Edit().Insert(v).Set(context.Background())
	}

	assert.Equal(len(vals), int(s.Len()))
	assert.True(bool(s.First(context.Background()).(Bool)))

	for _, v := range vals {
		assert.True(s.Has(context.Background(), v))
	}

	for len(vals) > 0 {
		v := vals[0]
		vals = vals[1:]
		s = s.Edit().Remove(v).Set(context.Background())
		assert.False(s.Has(context.Background(), v))
		assert.Equal(len(vals), int(s.Len()))
	}
}

func TestSetRemoveLastWhenNotLoaded(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()
	reload := func(s Set) Set {
		return vs.ReadValue(context.Background(), vs.WriteValue(context.Background(), s).TargetHash()).(Set)
	}

	ts := getTestNativeOrderSet(8, vs)
	ns := ts.toSet(vs)

	for len(ts) > 0 {
		last := ts[len(ts)-1]
		ts = ts[:len(ts)-1]
		ns = reload(ns.Edit().Remove(last).Set(context.Background()))
		assert.True(ts.toSet(vs).Equals(ns))
	}
}

func TestSetAt(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	values := []Value{Bool(false), Float(42), String("a"), String("b"), String("c")}
	s := NewSet(context.Background(), vs, values...)

	for i, v := range values {
		assert.Equal(v, s.At(context.Background(), uint64(i)))
	}

	assert.Panics(func() {
		s.At(context.Background(), 42)
	})
}

func TestSetWithStructShouldHaveOptionalFields(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	list := NewSet(context.Background(), vs,
		NewStruct(Format_7_18, "Foo", StructData{
			"a": Float(1),
		}),
		NewStruct(Format_7_18, "Foo", StructData{
			"a": Float(2),
			"b": String("bar"),
		}),
	)
	assert.True(
		MakeSetType(MakeStructType("Foo",
			StructField{"a", FloaTType, false},
			StructField{"b", StringType, true},
		),
		).Equals(TypeOf(list)))
}

func TestSetWithNil(t *testing.T) {
	vs := newTestValueStore()

	assert.Panics(t, func() {
		NewSet(context.Background(), vs, nil)
	})
	assert.Panics(t, func() {
		NewSet(context.Background(), vs, Float(42), nil)
	})
}
