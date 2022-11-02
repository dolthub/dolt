// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/dolthub/dolt/go/store/atomicerr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func (ts testSet) toSet(vrw ValueReadWriter) (Set, error) {
	return NewSet(context.Background(), vrw, ts...)
}

func newSortedTestSet(nbf *NomsBinFormat, length int, gen genValueFn) (values testSet) {
	for i := 0; i < length; i++ {
		values = append(values, mustValue(gen(nbf, i)))
	}
	return
}

func newTestSetFromSet(s Set) testSet {
	values := make([]Value, 0, s.Len())
	_ = s.IterAll(context.Background(), func(v Value) error {
		values = append(values, v)
		return nil
	})
	return values
}

func newRandomTestSet(nbf *NomsBinFormat, length int, gen genValueFn) testSet {
	s := rand.NewSource(4242)
	used := map[int]bool{}

	var values []Value
	for len(values) < length {
		v := int(s.Int63()) & 0xffffff
		if _, ok := used[v]; !ok {
			values = append(values, mustValue(gen(nbf, v)))
			used[v] = true
		}
	}

	return values
}

func validateSet(t *testing.T, vrw ValueReadWriter, s Set, values ValueSlice) {
	s, err := NewSet(context.Background(), vrw, values...)
	require.NoError(t, err)
	assert.True(t, s.Equals(s))
	out := ValueSlice{}
	_ = s.IterAll(context.Background(), func(v Value) error {
		out = append(out, v)
		return nil
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
	elemType := mustType(TypeOf(mustValue(gen(vs.Format(), 0))))
	elems := newSortedTestSet(vs.Format(), length, gen)
	tr := mustType(MakeSetType(elemType))
	set := mustSet(NewSet(context.Background(), vs, elems...))
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
				_ = l2.IterAll(context.Background(), func(v Value) error {
					out = append(out, v)
					return nil
				})
				exp := ValueSlice(elems)
				rv := exp.Equals(out)
				if !rv {
					printBadCollections(exp, out)
				}
				return rv
			},
			prependOne: func() (Collection, error) {
				dup := make([]Value, length+1)
				dup[0] = Float(-1)
				copy(dup[1:], elems)
				return NewSet(context.Background(), vs, dup...)
			},
			appendOne: func() (Collection, error) {
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

func (suite *setTestSuite) createStreamingSet(vs *ValueStore) (Set, error) {
	ae := atomicerr.New()
	vChan := make(chan Value)
	setChan := NewStreamingSet(context.Background(), vs, ae, vChan)
	for _, entry := range suite.elems {
		vChan <- entry
	}
	close(vChan)
	return <-setChan, ae.Get()
}

func (suite *setTestSuite) TestStreamingSet() {
	vs := newTestValueStore()
	defer vs.Close()
	s, err := suite.createStreamingSet(vs)
	suite.NoError(err)
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

	ae := atomicerr.New()
	readInput := func(vrw ValueReadWriter, vChan <-chan Value, outChan chan<- Set) {
		readSetInput(context.Background(), vrw, ae, vChan, outChan)
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
	var err1, err2 error
	go func() {
		s1, err1 = suite.createStreamingSet(vs)
		wg.Done()
	}()
	go func() {
		s2, err2 = suite.createStreamingSet(vs)
		wg.Done()
	}()
	wg.Wait()
	suite.NoError(err1)
	suite.True(suite.validate(s1))
	suite.NoError(err2)
	suite.True(suite.validate(s2))
}

func TestSetSuite4K(t *testing.T) {
	suite.Run(t, newSetTestSuite(12, 5, 2, 2, newNumber))
}

func TestSetSuite4KStructs(t *testing.T) {
	suite.Run(t, newSetTestSuite(12, 8, 2, 2, newNumberStruct))
}

func getTestNativeOrderSet(scale int, vrw ValueReadWriter) testSet {
	return newRandomTestSet(vrw.Format(), 64*scale, newNumber)
}

func getTestRefValueOrderSet(scale int, vrw ValueReadWriter) testSet {
	return newRandomTestSet(vrw.Format(), 64*scale, newNumber)
}

func getTestRefToNativeOrderSet(scale int, vrw ValueReadWriter) testSet {
	return newRandomTestSet(vrw.Format(), 64*scale, func(nbf *NomsBinFormat, v int) (Value, error) {
		return vrw.WriteValue(context.Background(), Float(v))
	})
}

func getTestRefToValueOrderSet(scale int, vrw ValueReadWriter) testSet {
	return newRandomTestSet(vrw.Format(), 64*scale, func(nbf *NomsBinFormat, v int) (Value, error) {
		return vrw.WriteValue(context.Background(), mustSet(NewSet(context.Background(), vrw, Float(v))))
	})
}

func accumulateSetDiffChanges(ctx context.Context, s1, s2 Set) (added []Value, removed []Value, err error) {
	changes := make(chan ValueChanged)
	go func() {
		defer close(changes)
		err = s1.Diff(ctx, s2, changes)
	}()
	for change := range changes {
		if change.ChangeType == DiffChangeAdded {
			added = append(added, change.Key)
		} else if change.ChangeType == DiffChangeRemoved {
			removed = append(removed, change.Key)
		}
	}
	return added, removed, err
}

func diffSetTest(assert *assert.Assertions, s1 Set, s2 Set, numAddsExpected int, numRemovesExpected int) (added []Value, removed []Value) {
	var err error
	added, removed, err = accumulateSetDiffChanges(context.Background(), s1, s2)
	assert.NoError(err)
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

	s, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	assert.True(mustType(MakeSetType(mustType(MakeUnionType()))).Equals(mustType(TypeOf(s))))
	assert.Equal(uint64(0), s.Len())

	s, err = NewSet(context.Background(), vs, Float(0))
	require.NoError(t, err)
	assert.True(mustType(MakeSetType(PrimitiveTypeMap[FloatKind])).Equals(mustType(TypeOf(s))))

	s, err = NewSet(context.Background(), vs)
	require.NoError(t, err)
	assert.IsType(mustType(MakeSetType(PrimitiveTypeMap[FloatKind])), mustType(TypeOf(s)))

	se, err := s.Edit().Remove(Float(1))
	require.NoError(t, err)
	s2, err := se.Set(context.Background())
	require.NoError(t, err)
	assert.IsType(mustType(TypeOf(s)), mustType(TypeOf(s2)))
}

func TestSetLen(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	s0, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	assert.Equal(uint64(0), s0.Len())
	s1, err := NewSet(context.Background(), vs, Bool(true), Float(1), String("hi"))
	require.NoError(t, err)
	assert.Equal(uint64(3), s1.Len())
	diffSetTest(assert, s0, s1, 0, 3)
	diffSetTest(assert, s1, s0, 3, 0)

	se2, err := s1.Edit().Insert(Bool(false))
	require.NoError(t, err)
	s2, err := se2.Set(context.Background())
	require.NoError(t, err)
	assert.Equal(uint64(4), s2.Len())
	diffSetTest(assert, s0, s2, 0, 4)
	diffSetTest(assert, s2, s0, 4, 0)
	diffSetTest(assert, s1, s2, 0, 1)
	diffSetTest(assert, s2, s1, 1, 0)

	se3, err := s2.Edit().Remove(Bool(true))
	require.NoError(t, err)
	s3, err := se3.Set(context.Background())
	require.NoError(t, err)
	assert.Equal(uint64(3), s3.Len())
	diffSetTest(assert, s2, s3, 1, 0)
	diffSetTest(assert, s3, s2, 0, 1)
}

func TestSetEmpty(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	assert.True(s.Empty())
	assert.Equal(uint64(0), s.Len())
}

func TestSetEmptyInsert(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	assert.True(s.Empty())
	se, err := s.Edit().Insert(Bool(false))
	require.NoError(t, err)
	s, err = se.Set(context.Background())
	require.NoError(t, err)
	assert.False(s.Empty())
	assert.Equal(uint64(1), s.Len())
}

func TestSetEmptyInsertRemove(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	assert.True(s.Empty())
	se, err := s.Edit().Insert(Bool(false))
	require.NoError(t, err)
	s, err = se.Set(context.Background())
	require.NoError(t, err)
	assert.False(s.Empty())
	assert.Equal(uint64(1), s.Len())
	se, err = s.Edit().Remove(Bool(false))
	require.NoError(t, err)
	s, err = se.Set(context.Background())
	require.NoError(t, err)
	assert.True(s.Empty())
	assert.Equal(uint64(0), s.Len())
}

// BUG 98
func TestSetDuplicateInsert(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s1, err := NewSet(context.Background(), vs, Bool(true), Float(42), Float(42))
	require.NoError(t, err)
	assert.Equal(uint64(2), s1.Len())
}

func TestSetUniqueKeysString(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s1, err := NewSet(context.Background(), vs, String("hello"), String("world"), String("hello"))
	require.NoError(t, err)
	assert.Equal(uint64(2), s1.Len())
	assert.True(s1.Has(context.Background(), String("hello")))
	assert.True(s1.Has(context.Background(), String("world")))
	assert.False(s1.Has(context.Background(), String("foo")))
}

func TestSetUniqueKeysNumber(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s1, err := NewSet(context.Background(), vs, Float(4), Float(1), Float(0), Float(0), Float(1), Float(3))
	require.NoError(t, err)
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

	s1, err := NewSet(context.Background(), vs, Bool(true), Float(1), String("hi"))
	require.NoError(t, err)
	assert.True(s1.Has(context.Background(), Bool(true)))
	assert.False(s1.Has(context.Background(), Bool(false)))
	assert.True(s1.Has(context.Background(), Float(1)))
	assert.False(s1.Has(context.Background(), Float(0)))
	assert.True(s1.Has(context.Background(), String("hi")))
	assert.False(s1.Has(context.Background(), String("ho")))

	se2, err := s1.Edit().Insert(Bool(false))
	require.NoError(t, err)
	s2, err := se2.Set(context.Background())
	require.NoError(t, err)
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
		set, err := ts.toSet(vrw)
		require.NoError(t, err)
		ref, err := vrw.WriteValue(context.Background(), set)
		require.NoError(t, err)
		val2, err := vrw.ReadValue(context.Background(), ref.TargetHash())
		require.NoError(t, err)
		set2 := val2.(Set)
		require.NoError(t, err)
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
	s, err := NewSet(context.Background(), vrw)
	require.NoError(t, err)
	for i, v := range values {
		se, err := s.Edit().Insert(v)
		require.NoError(t, err)
		s, err = se.Set(context.Background())
		require.NoError(t, err)
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

	validateSetInsertion(t, vs, generateNumbersAsValues(vs.Format(), 300))
}

func TestSetInsert(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	v1 := Bool(false)
	v2 := Bool(true)
	v3 := Float(0)

	assert.False(s.Has(context.Background(), v1))
	se, err := s.Edit().Insert(v1)
	require.NoError(t, err)
	s, err = se.Set(context.Background())
	require.NoError(t, err)
	assert.True(s.Has(context.Background(), v1))
	se, err = s.Edit().Insert(v2)
	require.NoError(t, err)
	s, err = se.Set(context.Background())
	require.NoError(t, err)
	assert.True(s.Has(context.Background(), v1))
	assert.True(s.Has(context.Background(), v2))
	se2, err := s.Edit().Insert(v3)
	require.NoError(t, err)
	s2, err := se2.Set(context.Background())
	require.NoError(t, err)
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
		expected, err := ts.toSet(vrw)
		require.NoError(t, err)
		run := func(from, to int) {
			s, err := ts.Remove(from, to).toSet(vrw)
			require.NoError(t, err)
			se, err := s.Edit().Insert(ts[from:to]...)
			require.NoError(t, err)
			actual, err := se.Set(context.Background())
			require.NoError(t, err)
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
	original, err := ts.toSet(vs)
	require.NoError(t, err)
	se, err := original.Edit().Insert(ts[0])
	require.NoError(t, err)
	actual, err := se.Set(context.Background())
	require.NoError(t, err)

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestSetRemove(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	v1 := Bool(false)
	v2 := Bool(true)
	v3 := Float(0)
	s, err := NewSet(context.Background(), vs, v1, v2, v3)
	require.NoError(t, err)
	assert.True(s.Has(context.Background(), v1))
	assert.True(s.Has(context.Background(), v2))
	assert.True(s.Has(context.Background(), v3))
	se, err := s.Edit().Remove(v1)
	require.NoError(t, err)
	s, err = se.Set(context.Background())
	require.NoError(t, err)
	assert.False(s.Has(context.Background(), v1))
	assert.True(s.Has(context.Background(), v2))
	assert.True(s.Has(context.Background(), v3))
	se2, err := s.Edit().Remove(v2)
	require.NoError(t, err)
	s2, err := se2.Set(context.Background())
	require.NoError(t, err)
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
		whole, err := ts.toSet(vrw)
		require.NoError(t, err)
		run := func(from, to int) {
			expected, err := ts.Remove(from, to).toSet(vrw)
			require.NoError(t, err)
			se, err := whole.Edit().Remove(ts[from:to]...)
			require.NoError(t, err)
			actual, err := se.Set(context.Background())
			require.NoError(t, err)
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
	original, err := ts.toSet(vs)
	require.NoError(t, err)
	se, err := original.Edit().Remove(Float(-1))
	require.NoError(t, err)
	actual, err := se.Set(context.Background()) // rand.Int63 returns non-negative values.
	require.NoError(t, err)

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestSetFirst(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	assert.Nil(s.First(context.Background()))
	se, err := s.Edit().Insert(Float(1))
	require.NoError(t, err)
	s, err = se.Set(context.Background())
	require.NoError(t, err)
	assert.NotNil(s.First(context.Background()))
	se, err = s.Edit().Insert(Float(2))
	require.NoError(t, err)
	s, err = se.Set(context.Background())
	require.NoError(t, err)
	assert.NotNil(s.First(context.Background()))
	se2, err := s.Edit().Remove(Float(1))
	require.NoError(t, err)
	s2, err := se2.Set(context.Background())
	require.NoError(t, err)
	assert.NotNil(s2.First(context.Background()))
	se2, err = s2.Edit().Remove(Float(2))
	require.NoError(t, err)
	s2, err = se2.Set(context.Background())
	require.NoError(t, err)
	assert.Nil(s2.First(context.Background()))
}

func TestSetOfStruct(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	elems := []Value{}
	for i := 0; i < 200; i++ {
		st, err := NewStruct(vs.Format(), "S1", StructData{"o": Float(i)})
		require.NoError(t, err)
		elems = append(elems, st)
	}

	s, err := NewSet(context.Background(), vs, elems...)
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		assert.True(s.Has(context.Background(), elems[i]))
	}
}

func TestSetIter(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s, err := NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3), Float(4))
	require.NoError(t, err)
	acc, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	err = s.Iter(context.Background(), func(v Value) (bool, error) {
		_, ok := v.(Float)
		assert.True(ok)
		se, err := acc.Edit().Insert(v)
		require.NoError(t, err)
		acc, err = se.Set(context.Background())
		require.NoError(t, err)
		return false, nil
	})
	require.NoError(t, err)
	assert.True(s.Equals(acc))

	acc, err = NewSet(context.Background(), vs)
	require.NoError(t, err)
	_ = s.Iter(context.Background(), func(v Value) (bool, error) {
		return true, nil
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
		set, err := ts.toSet(vrw)
		require.NoError(t, err)
		err = SortWithErroringLess(ValueSort{ts, vrw.Format()})
		require.NoError(t, err)
		idx := uint64(0)
		endAt := uint64(64)

		_ = set.Iter(context.Background(), func(v Value) (done bool, err error) {
			assert.True(ts[idx].Equals(v))
			if idx == endAt {
				done = true
			}
			idx++
			return done, nil
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

	s, err := NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3), Float(4))
	require.NoError(t, err)
	acc, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	_ = s.IterAll(context.Background(), func(v Value) error {
		_, ok := v.(Float)
		assert.True(ok)
		se, err := acc.Edit().Insert(v)
		require.NoError(t, err)
		acc, err = se.Set(context.Background())
		require.NoError(t, err)
		return nil
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
		set, err := ts.toSet(vrw)
		require.NoError(t, err)
		err = SortWithErroringLess(ValueSort{ts, vrw.Format()})
		require.NoError(t, err)
		idx := uint64(0)

		_ = set.IterAll(context.Background(), func(v Value) error {
			assert.True(ts[idx].Equals(v))
			idx++
			return nil
		})
	}

	doTest(getTestNativeOrderSet, 16)
	doTest(getTestRefValueOrderSet, 2)
	doTest(getTestRefToNativeOrderSet, 2)
	doTest(getTestRefToValueOrderSet, 2)
}

func testSetOrder(assert *assert.Assertions, vrw ValueReadWriter, valueType *Type, value []Value, expectOrdering []Value) {
	m, err := NewSet(context.Background(), vrw, value...)
	assert.NoError(err)
	i := 0
	_ = m.IterAll(context.Background(), func(value Value) error {
		expHsh, err := expectOrdering[i].Hash(vrw.Format())
		assert.NoError(err)
		hsh, err := value.Hash(vrw.Format())
		assert.NoError(err)
		assert.Equal(expHsh.String(), hsh.String())
		i++
		return nil
	})
}

func TestSetOrdering(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	testSetOrder(assert, vs,
		PrimitiveTypeMap[StringKind],
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
		PrimitiveTypeMap[FloatKind],
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
		PrimitiveTypeMap[FloatKind],
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
		PrimitiveTypeMap[FloatKind],
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
		PrimitiveTypeMap[ValueKind],
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
		PrimitiveTypeMap[BoolKind],
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

	s, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	assert.True(mustType(TypeOf(s)).Equals(mustType(MakeSetType(mustType(MakeUnionType())))))

	s, err = NewSet(context.Background(), vs, Float(0))
	require.NoError(t, err)
	assert.True(mustType(TypeOf(s)).Equals(mustType(MakeSetType(PrimitiveTypeMap[FloatKind]))))

	se2, err := s.Edit().Remove(Float(1))
	require.NoError(t, err)
	s2, err := se2.Set(context.Background())
	require.NoError(t, err)
	assert.True(mustType(TypeOf(s2)).Equals(mustType(MakeSetType(PrimitiveTypeMap[FloatKind]))))

	se2, err = s.Edit().Insert(Float(0), Float(1))
	require.NoError(t, err)
	s2, err = se2.Set(context.Background())
	require.NoError(t, err)
	assert.True(mustType(TypeOf(s)).Equals(mustType(TypeOf(s2))))

	se3, err := s.Edit().Insert(Bool(true))
	require.NoError(t, err)
	s3, err := se3.Set(context.Background())
	require.NoError(t, err)
	assert.True(mustType(TypeOf(s3)).Equals(mustType(MakeSetType(mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind]))))))
	se4, err := s.Edit().Insert(Float(3), Bool(true))
	require.NoError(t, err)
	s4, err := se4.Set(context.Background())
	require.NoError(t, err)
	assert.True(mustType(TypeOf(s4)).Equals(mustType(MakeSetType(mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind]))))))
}

func TestSetChunks(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	l1, err := NewSet(context.Background(), vs, Float(0))
	require.NoError(t, err)
	c1 := getChunks(vs.Format(), l1)
	assert.Len(c1, 0)

	ref, err := NewRef(Float(0), vs.Format())
	require.NoError(t, err)
	l2, err := NewSet(context.Background(), vs, ref)
	require.NoError(t, err)
	c2 := getChunks(vs.Format(), l2)
	assert.Len(c2, 1)
}

func TestSetChunks2(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(toTestSet toTestSetFunc, scale int) {
		vrw := newTestValueStore()
		ts := toTestSet(scale, vrw)
		set, err := ts.toSet(vrw)
		require.NoError(t, err)
		ref, err := vrw.WriteValue(context.Background(), set)
		require.NoError(t, err)
		val, err := vrw.ReadValue(context.Background(), ref.TargetHash())
		require.NoError(t, err)
		set2chunks := getChunks(vrw.Format(), val)
		for i, r := range getChunks(vrw.Format(), set) {
			assert.True(mustType(TypeOf(r)).Equals(mustType(TypeOf(set2chunks[i]))), "%s != %s", mustString(mustType(TypeOf(r)).Describe(context.Background())), mustString(mustType(TypeOf(set2chunks[i])).Describe(context.Background())))
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

	nums := generateNumbersAsValues(vs.Format(), testSetSize)
	s, err := NewSet(context.Background(), vs, nums...)
	require.NoError(t, err)
	assert.Equal(deriveCollectionHeight(s), getRefHeightOfCollection(s))
}

func TestSetRefOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)
	vs := newTestValueStore()

	nums := generateNumbersAsRefOfStructs(vs, testSetSize)
	s, err := NewSet(context.Background(), vs, nums...)
	require.NoError(t, err)
	// height + 1 because the leaves are Ref values (with height 1).
	assert.Equal(deriveCollectionHeight(s)+1, getRefHeightOfCollection(s))
}

func TestSetModifyAfterRead(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)
	vs := newTestValueStore()
	set, err := getTestNativeOrderSet(2, vs).toSet(vs)
	require.NoError(t, err)
	// Drop chunk values.
	ref, err := vs.WriteValue(context.Background(), set)
	require.NoError(t, err)
	val, err := vs.ReadValue(context.Background(), ref.TargetHash())
	require.NoError(t, err)
	set = val.(Set)
	require.NoError(t, err)
	// Modify/query. Once upon a time this would crash.
	fst, err := set.First(context.Background())
	require.NoError(t, err)
	se, err := set.Edit().Remove(fst)
	require.NoError(t, err)
	set, err = se.Set(context.Background())
	require.NoError(t, err)
	assert.False(set.Has(context.Background(), fst))
	val, err = set.First(context.Background())
	require.NoError(t, err)
	assert.True(set.Has(context.Background(), val))
	se, err = set.Edit().Insert(fst)
	require.NoError(t, err)
	set, err = se.Set(context.Background())
	require.NoError(t, err)
	assert.True(set.Has(context.Background(), fst))
}

func TestSetTypeAfterMutations(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	test := func(n int, c interface{}) {
		vs := newTestValueStore()
		values := generateNumbersAsValues(vs.Format(), n)

		s, err := NewSet(context.Background(), vs, values...)
		require.NoError(t, err)
		assert.Equal(s.Len(), uint64(n))
		assert.IsType(c, s.asSequence())
		assert.True(mustType(TypeOf(s)).Equals(mustType(MakeSetType(PrimitiveTypeMap[FloatKind]))))

		se, err := s.Edit().Insert(String("a"))
		require.NoError(t, err)
		s, err = se.Set(context.Background())
		require.NoError(t, err)
		assert.Equal(s.Len(), uint64(n+1))
		assert.IsType(c, s.asSequence())
		assert.True(mustType(TypeOf(s)).Equals(mustType(MakeSetType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind]))))))

		se, err = s.Edit().Remove(String("a"))
		require.NoError(t, err)
		s, err = se.Set(context.Background())
		require.NoError(t, err)
		assert.Equal(s.Len(), uint64(n))
		assert.IsType(c, s.asSequence())
		assert.True(mustType(TypeOf(s)).Equals(mustType(MakeSetType(PrimitiveTypeMap[FloatKind]))))
	}

	test(1, setLeafSequence{})
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
		mustValue(NewBlob(context.Background(), vs, bytes.NewBufferString("buf"))),
		mustValue(NewSet(context.Background(), vs, Bool(true))),
		mustValue(NewList(context.Background(), vs, Bool(true))),
		mustValue(NewMap(context.Background(), vs, Bool(true), Float(0))),
		mustValue(NewStruct(vs.Format(), "", StructData{"field": Bool(true)})),
		// Refs of values
		mustValue(NewRef(Bool(true), vs.Format())),
		mustValue(NewRef(Float(0), vs.Format())),
		mustValue(NewRef(String("hello"), vs.Format())),
		mustValue(NewRef(mustValue(NewBlob(context.Background(), vs, bytes.NewBufferString("buf"))), vs.Format())),
		mustValue(NewRef(mustValue(NewSet(context.Background(), vs, Bool(true))), vs.Format())),
		mustValue(NewRef(mustValue(NewList(context.Background(), vs, Bool(true))), vs.Format())),
		mustValue(NewRef(mustValue(NewMap(context.Background(), vs, Bool(true), Float(0))), vs.Format())),
		mustValue(NewRef(mustValue(NewStruct(vs.Format(), "", StructData{"field": Bool(true)})), vs.Format())),
	}

	s, err := NewSet(context.Background(), vs, vals...)
	require.NoError(t, err)
	for i := 1; s.asSequence().isLeaf(); i++ {
		v := Float(i)
		vals = append(vals, v)
		se, err := s.Edit().Insert(v)
		require.NoError(t, err)
		s, err = se.Set(context.Background())
		require.NoError(t, err)
	}

	assert.Equal(len(vals), int(s.Len()))
	assert.True(bool(mustValue(s.First(context.Background())).(Bool)))

	for _, v := range vals {
		assert.True(s.Has(context.Background(), v))
	}

	for len(vals) > 0 {
		v := vals[0]
		vals = vals[1:]
		se, err := s.Edit().Remove(v)
		require.NoError(t, err)
		s, err = se.Set(context.Background())
		require.NoError(t, err)
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
		ref, err := vs.WriteValue(context.Background(), s)
		require.NoError(t, err)
		v, err := vs.ReadValue(context.Background(), ref.TargetHash())
		require.NoError(t, err)
		return v.(Set)
	}

	ts := getTestNativeOrderSet(8, vs)
	ns, err := ts.toSet(vs)
	require.NoError(t, err)

	for len(ts) > 0 {
		last := ts[len(ts)-1]
		ts = ts[:len(ts)-1]
		se, err := ns.Edit().Remove(last)
		require.NoError(t, err)
		s, err := se.Set(context.Background())
		require.NoError(t, err)
		ns = reload(s)
		assert.True(mustSet(ts.toSet(vs)).Equals(ns))
	}
}

func TestSetAt(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	values := []Value{Bool(false), Float(42), String("a"), String("b"), String("c")}
	s, err := NewSet(context.Background(), vs, values...)
	require.NoError(t, err)

	for i, v := range values {
		valAt, err := s.At(context.Background(), uint64(i))
		require.NoError(t, err)
		assert.Equal(v, valAt)
	}

	assert.Panics(func() {
		_, _ = s.At(context.Background(), 42)
	})
}

func TestSetWithStructShouldHaveOptionalFields(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	list, err := NewSet(context.Background(), vs,
		mustValue(NewStruct(vs.Format(), "Foo", StructData{
			"a": Float(1),
		})),
		mustValue(NewStruct(vs.Format(), "Foo", StructData{
			"a": Float(2),
			"b": String("bar"),
		})),
	)
	require.NoError(t, err)
	assert.True(
		mustType(MakeSetType(mustType(MakeStructType("Foo",
			StructField{"a", PrimitiveTypeMap[FloatKind], false},
			StructField{"b", PrimitiveTypeMap[StringKind], true},
		),
		))).Equals(mustType(TypeOf(list))))
}

func TestSetWithNil(t *testing.T) {
	vs := newTestValueStore()

	assert.Panics(t, func() {
		_, _ = NewSet(context.Background(), vs, nil)
	})
	assert.Panics(t, func() {
		_, _ = NewSet(context.Background(), vs, Float(42), nil)
	})
}
