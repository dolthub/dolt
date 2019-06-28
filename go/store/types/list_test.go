// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"math/rand"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const testListSize = 5000

type testList ValueSlice

func (tl testList) AsValuables() []Valuable {
	vs := make([]Valuable, len(tl))
	for i, v := range tl {
		vs[i] = v
	}
	return vs
}

func (tl testList) Set(idx int, v Value) (res testList) {
	return tl.Splice(idx, 1, v)
}

func (tl testList) Insert(idx int, vs ...Value) testList {
	return tl.Splice(idx, 0, vs...)
}

func (tl testList) Remove(start, end int) testList {
	return tl.Splice(start, end-start)
}

func (tl testList) RemoveAt(idx int) testList {
	return tl.Splice(idx, 1)
}

func (tl testList) Splice(idx int, remove int, insert ...Value) (res testList) {
	res = append(res, tl[:idx]...)
	res = append(res, insert...)
	res = append(res, tl[idx+remove:]...)
	return
}

func (tl testList) Diff(last testList) []Splice {
	// Note: this could be use tl.toList/last.toList and then tlList.Diff(lastList)
	// but the purpose of this method is to be redundant.
	return calcSplices(uint64(len(last)), uint64(len(tl)), DEFAULT_MAX_SPLICE_MATRIX_SIZE,
		func(i uint64, j uint64) bool { return last[i] == tl[j] })
}

func (tl testList) toList(vrw ValueReadWriter) List {
	// TODO(binformat)
	return NewList(context.Background(), Format_7_18, vrw, tl...)
}

func newTestList(length int) testList {
	return generateNumbersAsValues(length)
}

func validateList(t *testing.T, vrw ValueReadWriter, l List, values ValueSlice) {
	// TODO(binformat)
	assert.True(t, l.Equals(NewList(context.Background(), Format_7_18, vrw, values...)))
	out := ValueSlice{}
	l.IterAll(context.Background(), func(v Value, idx uint64) {
		out = append(out, v)
	})
	assert.True(t, out.Equals(values))
}

type listTestSuite struct {
	collectionTestSuite
	elems testList
}

func newListTestSuite(size uint, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int) *listTestSuite {
	vrw := newTestValueStore()

	length := 1 << size
	elems := newTestList(length)
	tr := MakeListType(FloaTType)
	// TODO(binformat)
	list := NewList(context.Background(), Format_7_18, vrw, elems...)
	return &listTestSuite{
		collectionTestSuite: collectionTestSuite{
			col:                    list,
			expectType:             tr,
			expectLen:              uint64(length),
			expectChunkCount:       expectChunkCount,
			expectPrependChunkDiff: expectPrependChunkDiff,
			expectAppendChunkDiff:  expectAppendChunkDiff,
			validate: func(v2 Collection) bool {
				l2 := v2.(List)
				out := ValueSlice{}
				l2.IterAll(context.Background(), func(v Value, index uint64) {
					out = append(out, v)
				})
				return ValueSlice(elems).Equals(out)
			},
			prependOne: func() Collection {
				dup := make([]Value, length+1)
				dup[0] = Float(0)
				copy(dup[1:], elems)
				// TODO(binformat)
				return NewList(context.Background(), Format_7_18, vrw, dup...)
			},
			appendOne: func() Collection {
				dup := make([]Value, length+1)
				copy(dup, elems)
				dup[len(dup)-1] = Float(0)
				// TODO(binformat)
				return NewList(context.Background(), Format_7_18, vrw, dup...)
			},
		},
		elems: elems,
	}
}

func (suite *listTestSuite) TestGet() {
	list := suite.col.(List)
	for i := 0; i < len(suite.elems); i++ {
		suite.True(suite.elems[i].Equals(list.Get(context.Background(), uint64(i))))
	}
	suite.Equal(suite.expectLen, list.Len())
}

func (suite *listTestSuite) TestIter() {
	list := suite.col.(List)
	expectIdx := uint64(0)
	endAt := suite.expectLen / 2
	list.Iter(context.Background(), func(v Value, idx uint64) bool {
		suite.Equal(expectIdx, idx)
		expectIdx++
		suite.Equal(suite.elems[idx], v)
		return expectIdx == endAt
	})

	suite.Equal(endAt, expectIdx)
}

func (suite *listTestSuite) TestIterRange() {
	list := suite.col.(List)

	for s := uint64(0); s < 6; s++ {
		batchSize := list.Len() / (2 << s)
		expectIdx := uint64(0)
		for i := uint64(0); i < list.Len(); i += batchSize {
			// TODO(binformat)
			list.IterRange(context.Background(), Format_7_18, i, i+batchSize, func(v Value, idx uint64) {
				suite.Equal(expectIdx, idx)
				expectIdx++
				suite.Equal(suite.elems[idx], v)
			})
		}
	}
}

func TestListSuite4K(t *testing.T) {
	suite.Run(t, newListTestSuite(12, 8, 2, 2))
}

func TestListSuite8K(t *testing.T) {
	suite.Run(t, newListTestSuite(14, 22, 2, 2))
}

func TestListInsert(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()
	vrw := newTestValueStore()

	assert := assert.New(t)

	tl := newTestList(1024)
	list := tl.toList(vrw)

	for i := 0; i < len(tl); i += 16 {
		tl = tl.Insert(i, Float(i))
		list = list.Edit().Insert(uint64(i), Float(i)).List(context.Background())
	}

	assert.True(tl.toList(vrw).Equals(list))
}

func TestListRemove(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)

	tl := newTestList(1024)
	list := tl.toList(vrw)

	for i := len(tl) - 16; i >= 0; i -= 16 {
		tl = tl.Remove(i, i+4)
		list = list.Edit().Remove(uint64(i), uint64(i+4)).List(context.Background())
	}

	assert.True(tl.toList(vrw).Equals(list))
}

func TestListRemoveAt(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	// TODO(binformat)
	l0 := NewList(context.Background(), Format_7_18, vrw)
	l0 = l0.Edit().Append(Bool(false), Bool(true)).List(context.Background())
	l1 := l0.Edit().RemoveAt(1).List(context.Background())
	assert.True(NewList(context.Background(), Format_7_18, vrw, Bool(false)).Equals(l1))
	l1 = l1.Edit().RemoveAt(0).List(context.Background())
	assert.True(NewList(context.Background(), Format_7_18, vrw).Equals(l1))

	assert.Panics(func() {
		l1.Edit().RemoveAt(0).List(context.Background())
	})
}

func getTestListLen() uint64 {
	return uint64(64) * 50
}

func getTestList() testList {
	return getTestListWithLen(int(getTestListLen()))
}

func getTestListWithLen(length int) testList {
	s := rand.NewSource(42)
	values := make([]Value, length)
	for i := 0; i < length; i++ {
		values[i] = Float(s.Int63() & 0xff)
	}

	return values
}

func testListFromNomsList(list List) testList {
	simple := make(testList, list.Len())
	list.IterAll(context.Background(), func(v Value, offset uint64) {
		simple[offset] = v
	})
	return simple
}

func TestStreamingListCreation(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	vs := newTestValueStore()
	simpleList := getTestList()

	// TODO(binformat)
	cl := NewList(context.Background(), Format_7_18, vs, simpleList...)
	valueChan := make(chan Value)
	listChan := NewStreamingList(context.Background(), Format_7_18, vs, valueChan)
	for _, v := range simpleList {
		valueChan <- v
	}
	close(valueChan)
	sl := <-listChan
	assert.True(cl.Equals(sl))
	cl.Iter(context.Background(), func(v Value, idx uint64) (done bool) {
		done = !assert.True(v.Equals(sl.Get(context.Background(), idx)))
		return
	})
}

func TestListAppend(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	// TODO(binformat)
	newList := func(items testList) List {
		return NewList(context.Background(), Format_7_18, vrw, items...)
	}

	listToSimple := func(cl List) (simple testList) {
		cl.IterAll(context.Background(), func(v Value, offset uint64) {
			simple = append(simple, v)
		})
		return
	}

	cl := newList(getTestList())
	cl2 := cl.Edit().Append(Float(42)).List(context.Background())
	cl3 := cl2.Edit().Append(Float(43)).List(context.Background())
	cl4 := cl3.Edit().Append(getTestList().AsValuables()...).List(context.Background())
	cl5 := cl4.Edit().Append(Float(44), Float(45)).List(context.Background())
	cl6 := cl5.Edit().Append(getTestList().AsValuables()...).List(context.Background())

	expected := getTestList()
	assert.Equal(expected, listToSimple(cl))
	assert.Equal(getTestListLen(), cl.Len())
	assert.True(newList(expected).Equals(cl))

	expected = append(expected, Float(42))
	assert.Equal(expected, listToSimple(cl2))
	assert.Equal(getTestListLen()+1, cl2.Len())
	assert.True(newList(expected).Equals(cl2))

	expected = append(expected, Float(43))
	assert.Equal(expected, listToSimple(cl3))
	assert.Equal(getTestListLen()+2, cl3.Len())
	assert.True(newList(expected).Equals(cl3))

	expected = append(expected, getTestList()...)
	assert.Equal(expected, listToSimple(cl4))
	assert.Equal(2*getTestListLen()+2, cl4.Len())
	assert.True(newList(expected).Equals(cl4))

	expected = append(expected, Float(44), Float(45))
	assert.Equal(expected, listToSimple(cl5))
	assert.Equal(2*getTestListLen()+4, cl5.Len())
	assert.True(newList(expected).Equals(cl5))

	expected = append(expected, getTestList()...)
	assert.Equal(expected, listToSimple(cl6))
	assert.Equal(3*getTestListLen()+4, cl6.Len())
	assert.True(newList(expected).Equals(cl6))
}

func TestListValidateInsertAscending(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	values := generateNumbersAsValues(1000)

	// TODO(binformat)
	s := NewList(context.Background(), Format_7_18, vrw)
	for i, v := range values {
		s = s.Edit().Insert(uint64(i), v).List(context.Background())
		validateList(t, vrw, s, values[0:i+1])
	}
}

func TestListValidateInsertAtZero(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	values := generateNumbersAsValues(1000)
	// TODO(binformat)
	s := NewList(context.Background(), Format_7_18, vrw)
	count := len(values)
	for count > 0 {
		count--
		v := values[count]
		s = s.Edit().Insert(uint64(0), v).List(context.Background())
		validateList(t, vrw, s, values[count:])
	}
}

func TestListInsertNothing(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	cl := getTestList().toList(vrw)

	assert.True(cl.Equals(cl.Edit().Insert(0).List(context.Background())))
	for i := uint64(1); i < getTestListLen(); i *= 2 {
		assert.True(cl.Equals(cl.Edit().Insert(i).List(context.Background())))
	}
	assert.True(cl.Equals(cl.Edit().Insert(cl.Len() - 1).List(context.Background())))
	assert.True(cl.Equals(cl.Edit().Insert(cl.Len()).List(context.Background())))
}

func TestListInsertStart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	cl := getTestList().toList(vrw)
	cl2 := cl.Edit().Insert(0, Float(42)).List(context.Background())
	cl3 := cl2.Edit().Insert(0, Float(43)).List(context.Background())
	cl4 := cl3.Edit().Insert(0, getTestList().AsValuables()...).List(context.Background())
	cl5 := cl4.Edit().Insert(0, Float(44), Float(45)).List(context.Background())
	cl6 := cl5.Edit().Insert(0, getTestList().AsValuables()...).List(context.Background())

	expected := getTestList()
	assert.Equal(expected, testListFromNomsList(cl))
	assert.Equal(getTestListLen(), cl.Len())
	assert.True(expected.toList(vrw).Equals(cl))

	expected = expected.Insert(0, Float(42))
	assert.Equal(expected, testListFromNomsList(cl2))
	assert.Equal(getTestListLen()+1, cl2.Len())
	assert.True(expected.toList(vrw).Equals(cl2))

	expected = expected.Insert(0, Float(43))
	assert.Equal(expected, testListFromNomsList(cl3))
	assert.Equal(getTestListLen()+2, cl3.Len())
	assert.True(expected.toList(vrw).Equals(cl3))

	expected = expected.Insert(0, getTestList()...)
	assert.Equal(expected, testListFromNomsList(cl4))
	assert.Equal(2*getTestListLen()+2, cl4.Len())
	assert.True(expected.toList(vrw).Equals(cl4))

	expected = expected.Insert(0, Float(44), Float(45))
	assert.Equal(expected, testListFromNomsList(cl5))
	assert.Equal(2*getTestListLen()+4, cl5.Len())
	assert.True(expected.toList(vrw).Equals(cl5))

	expected = expected.Insert(0, getTestList()...)
	assert.Equal(expected, testListFromNomsList(cl6))
	assert.Equal(3*getTestListLen()+4, cl6.Len())
	assert.True(expected.toList(vrw).Equals(cl6))
}

func TestListInsertMiddle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	cl := getTestList().toList(vrw)
	cl2 := cl.Edit().Insert(100, Float(42)).List(context.Background())
	cl3 := cl2.Edit().Insert(200, Float(43)).List(context.Background())
	cl4 := cl3.Edit().Insert(300, getTestList().AsValuables()...).List(context.Background())
	cl5 := cl4.Edit().Insert(400, Float(44), Float(45)).List(context.Background())
	cl6 := cl5.Edit().Insert(500, getTestList().AsValuables()...).List(context.Background())
	cl7 := cl6.Edit().Insert(600, Float(100)).List(context.Background())

	expected := getTestList()
	assert.Equal(expected, testListFromNomsList(cl))
	assert.Equal(getTestListLen(), cl.Len())
	assert.True(expected.toList(vrw).Equals(cl))

	expected = expected.Insert(100, Float(42))
	assert.Equal(expected, testListFromNomsList(cl2))
	assert.Equal(getTestListLen()+1, cl2.Len())
	assert.True(expected.toList(vrw).Equals(cl2))

	expected = expected.Insert(200, Float(43))
	assert.Equal(expected, testListFromNomsList(cl3))
	assert.Equal(getTestListLen()+2, cl3.Len())
	assert.True(expected.toList(vrw).Equals(cl3))

	expected = expected.Insert(300, getTestList()...)
	assert.Equal(expected, testListFromNomsList(cl4))
	assert.Equal(2*getTestListLen()+2, cl4.Len())
	assert.True(expected.toList(vrw).Equals(cl4))

	expected = expected.Insert(400, Float(44), Float(45))
	assert.Equal(expected, testListFromNomsList(cl5))
	assert.Equal(2*getTestListLen()+4, cl5.Len())
	assert.True(expected.toList(vrw).Equals(cl5))

	expected = expected.Insert(500, getTestList()...)
	assert.Equal(expected, testListFromNomsList(cl6))
	assert.Equal(3*getTestListLen()+4, cl6.Len())
	assert.True(expected.toList(vrw).Equals(cl6))

	expected = expected.Insert(600, Float(100))
	assert.Equal(expected, testListFromNomsList(cl7))
	assert.Equal(3*getTestListLen()+5, cl7.Len())
	assert.True(expected.toList(vrw).Equals(cl7))
}

func TestListInsertRanges(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	testList := getTestList()
	whole := testList.toList(vrw)

	// Compare list equality. Increment by 256 (16^2) because each iteration requires building a new list, which is slow.
	for incr, i := 256, 0; i < len(testList)-incr; i += incr {
		for window := 1; window <= incr; window *= 16 {
			testListPart := testList.Remove(i, i+window)
			actual := testListPart.toList(vrw).Edit().Insert(uint64(i), testList[i:i+window].AsValuables()...).List(context.Background())
			assert.Equal(whole.Len(), actual.Len())
			assert.True(whole.Equals(actual))
		}
	}

	// Compare list length, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList); i += incr {
		assert.Equal(len(testList)+incr, int(whole.Edit().Insert(uint64(i), testList[0:incr].AsValuables()...).List(context.Background()).Len()))
	}
}

func TestListRemoveNothing(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	cl := getTestList().toList(vrw)

	assert.True(cl.Equals(cl.Edit().Remove(0, 0).List(context.Background())))
	for i := uint64(1); i < getTestListLen(); i *= 2 {
		assert.True(cl.Equals(cl.Edit().Remove(i, i).List(context.Background())))
	}
	assert.True(cl.Equals(cl.Edit().Remove(cl.Len()-1, cl.Len()-1).List(context.Background())))
	assert.True(cl.Equals(cl.Edit().Remove(cl.Len(), cl.Len()).List(context.Background())))
}

func TestListRemoveEverything(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	// TODO(binformat)
	cl := getTestList().toList(vrw).Edit().Remove(0, getTestListLen()).List(context.Background())

	// TODO(binformat)
	assert.True(NewList(context.Background(), Format_7_18, vrw).Equals(cl))
	assert.Equal(0, int(cl.Len()))
}

func TestListRemoveAtMiddle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	cl := getTestList().toList(vrw)
	cl2 := cl.Edit().RemoveAt(100).List(context.Background())
	cl3 := cl2.Edit().RemoveAt(200).List(context.Background())

	expected := getTestList()
	assert.Equal(expected, testListFromNomsList(cl))
	assert.Equal(getTestListLen(), cl.Len())
	assert.True(expected.toList(vrw).Equals(cl))

	expected = expected.RemoveAt(100)
	assert.Equal(expected, testListFromNomsList(cl2))
	assert.Equal(getTestListLen()-1, cl2.Len())
	assert.True(expected.toList(vrw).Equals(cl2))

	expected = expected.RemoveAt(200)
	assert.Equal(expected, testListFromNomsList(cl3))
	assert.Equal(getTestListLen()-2, cl3.Len())
	assert.True(expected.toList(vrw).Equals(cl3))
}

func TestListRemoveRanges(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	testList := getTestList()
	whole := testList.toList(vrw)

	// Compare list equality. Increment by 256 (16^2) because each iteration requires building a new list, which is slow.
	for incr, i := 256, 0; i < len(testList)-incr; i += incr {
		for window := 1; window <= incr; window *= 16 {
			testListPart := testList.Remove(i, i+window)
			expected := testListPart.toList(vrw)
			actual := whole.Edit().Remove(uint64(i), uint64(i+window)).List(context.Background())
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
		}
	}

	// Compare list length, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList)-incr; i += incr {
		assert.Equal(len(testList)-incr, int(whole.Edit().Remove(uint64(i), uint64(i+incr)).List(context.Background()).Len()))
	}
}

func TestListRemoveAtEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	tl := getTestListWithLen(testListSize / 10)
	cl := tl.toList(vrw)

	for i := len(tl) - 1; i >= 0; i-- {
		cl = cl.Edit().Remove(uint64(i), uint64(i+1)).List(context.Background())
		expect := tl[0:i].toList(vrw)
		assert.True(expect.Equals(cl))
	}
}

func TestListSet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	testList := getTestList()
	cl := testList.toList(vrw)

	testIdx := func(idx int, testEquality bool) {
		newVal := Float(-1) // Test values are never < 0
		cl2 := cl.Edit().Set(uint64(idx), newVal).List(context.Background())
		assert.False(cl.Equals(cl2))
		if testEquality {
			assert.True(testList.Set(idx, newVal).toList(vrw).Equals(cl2))
		}
	}

	// Compare list equality. Increment by 100 because each iteration requires building a new list, which is slow, but always test the last index.
	for incr, i := 100, 0; i < len(testList); i += incr {
		testIdx(i, true)
	}
	testIdx(len(testList)-1, true)

	// Compare list unequality, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList); i += incr {
		testIdx(i, false)
	}
}

func TestListFirstNNumbers(t *testing.T) {
	vrw := newTestValueStore()

	nums := generateNumbersAsValues(testListSize)
	// TODO(binformat)
	NewList(context.Background(), Format_7_18, vrw, nums...)
}

func TestListRefOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	vrw := newTestValueStore()

	nums := generateNumbersAsRefOfStructs(vrw, testListSize)
	// TODO(binformat)
	NewList(context.Background(), Format_7_18, vrw, nums...)
}

func TestListModifyAfterRead(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()

	list := getTestList().toList(vs)
	// Drop chunk values.
	list = vs.ReadValue(context.Background(), vs.WriteValue(context.Background(), list).TargetHash()).(List)
	// Modify/query. Once upon a time this would crash.
	llen := list.Len()
	z := list.Get(context.Background(), 0)
	list = list.Edit().RemoveAt(0).List(context.Background())
	assert.Equal(llen-1, list.Len())
	list = list.Edit().Append(z).List(context.Background())
	assert.Equal(llen, list.Len())
}

func accumulateDiffSplices(l1, l2 List) (diff []Splice) {
	diffChan := make(chan Splice)
	go func() {
		l1.Diff(context.Background(), l2, diffChan, nil)
		close(diffChan)
	}()
	for splice := range diffChan {
		diff = append(diff, splice)
	}
	return
}

func accumulateDiffSplicesWithLimit(l1, l2 List, maxSpliceMatrixSize uint64) (diff []Splice) {
	diffChan := make(chan Splice)
	go func() {
		l1.DiffWithLimit(context.Background(), l2, diffChan, nil, maxSpliceMatrixSize)
		close(diffChan)
	}()
	for splice := range diffChan {
		diff = append(diff, splice)
	}
	return diff
}

func TestListDiffIdentical(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)
	nums := generateNumbersAsValues(5)
	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, nums...)
	l2 := NewList(context.Background(), Format_7_18, vrw, nums...)

	diff1 := accumulateDiffSplices(l1, l2)
	diff2 := accumulateDiffSplices(l2, l1)

	assert.Equal(0, len(diff1))
	assert.Equal(0, len(diff2))
}

func TestListDiffVersusEmpty(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5)
	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, nums1...)
	l2 := NewList(context.Background(), Format_7_18, vrw)

	diff1 := accumulateDiffSplices(l1, l2)
	diff2 := accumulateDiffSplices(l2, l1)

	assert.Equal(len(diff2), len(diff1))
	diffExpected := []Splice{
		{0, 0, 5, 0},
	}
	assert.Equal(diffExpected, diff1, "expected diff is wrong")
}

func TestListDiffReverse(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5000)
	nums2 := reverseValues(nums1)
	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, nums1...)
	l2 := NewList(context.Background(), Format_7_18, vrw, nums2...)

	diff1 := accumulateDiffSplices(l1, l2)
	diff2 := accumulateDiffSplices(l2, l1)

	diffExpected := []Splice{
		{0, 5000, 5000, 0},
	}
	assert.Equal(diffExpected, diff1, "expected diff is wrong")
	assert.Equal(diffExpected, diff2, "expected diff is wrong")
}

func TestListDiffReverseWithLargerLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5000)
	nums2 := reverseValues(nums1)

	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, nums1...)
	l2 := NewList(context.Background(), Format_7_18, vrw, nums2...)

	diff1 := accumulateDiffSplicesWithLimit(l1, l2, 27e6)
	diff2 := accumulateDiffSplicesWithLimit(l2, l1, 27e6)

	assert.Equal(len(diff2), len(diff1))
	diffExpected := []Splice{
		{0, 2499, 2500, 0},
		{2500, 2500, 2499, 2501},
	}
	assert.Equal(diffExpected, diff1, "expected diff is wrong")
	assert.Equal(diffExpected, diff2, "expected diff is wrong")
}

func TestListDiffRemove5x100(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5000)
	nums2 := generateNumbersAsValues(5000)
	for count := 5; count > 0; count-- {
		nums2 = spliceValues(nums2, (count-1)*1000, 100)
	}
	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, nums1...)
	l2 := NewList(context.Background(), Format_7_18, vrw, nums2...)

	diff1 := accumulateDiffSplices(l1, l2)
	diff2 := accumulateDiffSplices(l2, l1)

	assert.Equal(len(diff1), len(diff2))
	diff2Expected := []Splice{
		{0, 100, 0, 0},
		{1000, 100, 0, 0},
		{2000, 100, 0, 0},
		{3000, 100, 0, 0},
		{4000, 100, 0, 0},
	}
	assert.Equal(diff2Expected, diff2, "expected diff is wrong")
}

func TestListDiffAdd5x5(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5000)
	nums2 := generateNumbersAsValues(5000)
	for count := 5; count > 0; count-- {
		nums2 = spliceValues(nums2, (count-1)*1000, 0, Float(0), Float(1), Float(2), Float(3), Float(4))
	}
	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, nums1...)
	l2 := NewList(context.Background(), Format_7_18, vrw, nums2...)

	diff1 := accumulateDiffSplices(l1, l2)
	diff2 := accumulateDiffSplices(l2, l1)

	assert.Equal(len(diff1), len(diff2))
	diff2Expected := []Splice{
		{5, 0, 5, 5},
		{1000, 0, 5, 1005},
		{2000, 0, 5, 2010},
		{3000, 0, 5, 3015},
		{4000, 0, 5, 4020},
	}
	assert.Equal(diff2Expected, diff2, "expected diff is wrong")
}

func TestListDiffReplaceReverse5x100(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5000)
	nums2 := generateNumbersAsValues(5000)
	for count := 5; count > 0; count-- {
		out := reverseValues(nums2[(count-1)*1000 : (count-1)*1000+100])
		nums2 = spliceValues(nums2, (count-1)*1000, 100, out...)
	}
	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, nums1...)
	l2 := NewList(context.Background(), Format_7_18, vrw, nums2...)
	diff := accumulateDiffSplices(l2, l1)

	diffExpected := []Splice{
		{0, 49, 50, 0},
		{50, 50, 49, 51},
		{1000, 49, 50, 1000},
		{1050, 50, 49, 1051},
		{2000, 49, 50, 2000},
		{2050, 50, 49, 2051},
		{3000, 49, 50, 3000},
		{3050, 50, 49, 3051},
		{4000, 49, 50, 4000},
		{4050, 50, 49, 4051},
	}
	assert.Equal(diffExpected, diff, "expected diff is wrong")
}

func TestListDiffString1(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)
	nums1 := []Value{String("one"), String("two"), String("three")}
	nums2 := []Value{String("one"), String("two"), String("three")}
	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, nums1...)
	l2 := NewList(context.Background(), Format_7_18, vrw, nums2...)
	diff := accumulateDiffSplices(l2, l1)

	assert.Equal(0, len(diff))
}

func TestListDiffString2(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)
	nums1 := []Value{String("one"), String("two"), String("three")}
	nums2 := []Value{String("one"), String("two"), String("three"), String("four")}
	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, nums1...)
	l2 := NewList(context.Background(), Format_7_18, vrw, nums2...)
	diff := accumulateDiffSplices(l2, l1)

	diffExpected := []Splice{
		{3, 0, 1, 3},
	}
	assert.Equal(diffExpected, diff, "expected diff is wrong")
}

func TestListDiffString3(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)
	nums1 := []Value{String("one"), String("two"), String("three")}
	nums2 := []Value{String("one"), String("two"), String("four")}
	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, nums1...)
	l2 := NewList(context.Background(), Format_7_18, vrw, nums2...)
	diff := accumulateDiffSplices(l2, l1)

	diffExpected := []Splice{
		{2, 1, 1, 2},
	}
	assert.Equal(diffExpected, diff, "expected diff is wrong")
}

func TestListDiffLargeWithSameMiddle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	assert := assert.New(t)

	storage := &chunks.TestStorage{}

	cs1 := storage.NewView()
	vs1 := NewValueStore(cs1)
	nums1 := generateNumbersAsValues(4000)
	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vs1, nums1...)
	hash1 := vs1.WriteValue(context.Background(), l1).TargetHash()
	vs1.Commit(context.Background(), vs1.Root(context.Background()), vs1.Root(context.Background()))

	refList1 := vs1.ReadValue(context.Background(), hash1).(List)

	cs2 := storage.NewView()
	vs2 := NewValueStore(cs2)
	nums2 := generateNumbersAsValuesFromToBy(5, 3550, 1)
	// TODO(binformat)
	l2 := NewList(context.Background(), Format_7_18, vs2, nums2...)
	hash2 := vs2.WriteValue(context.Background(), l2).TargetHash()
	vs2.Commit(context.Background(), vs1.Root(context.Background()), vs1.Root(context.Background()))
	refList2 := vs2.ReadValue(context.Background(), hash2).(List)

	// diff lists without value store
	diff1 := accumulateDiffSplices(l2, l1)
	assert.Equal(2, len(diff1))

	// diff lists from value stores
	diff2 := accumulateDiffSplices(refList2, refList1)
	assert.Equal(2, len(diff2))

	// diff without and with value store should be same
	assert.Equal(diff1, diff2)

	// should only read/write a "small & reasonably sized portion of the total"
	assert.Equal(9, cs1.Writes)
	assert.Equal(3, cs1.Reads)
	assert.Equal(9, cs2.Writes)
	assert.Equal(3, cs2.Reads)
}

func TestListDiffAllValuesInSequenceRemoved(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	newSequenceMetaTuple := func(vs ...Value) metaTuple {
		// TODO(binformat)
		seq := newListLeafSequence(vrw, Format_7_18, vs...)
		list := newList(seq, Format_7_18)
		return newMetaTuple(Format_7_18, vrw.WriteValue(context.Background(), list), orderedKeyFromInt(len(vs), Format_7_18), uint64(len(vs)))
	}

	m1 := newSequenceMetaTuple(Float(1), Float(2), Float(3))
	m2 := newSequenceMetaTuple(Float(4), Float(5), Float(6), Float(7), Float(8))
	m3 := newSequenceMetaTuple(Float(9), Float(10), Float(11), Float(12), Float(13), Float(14), Float(15))

	l1 := newList(newListMetaSequence(1, []metaTuple{m1, m3}, Format_7_18, vrw), Format_7_18)     // [1, 2, 3][9, 10, 11, 12, 13, 14, 15]
	l2 := newList(newListMetaSequence(1, []metaTuple{m1, m2, m3}, Format_7_18, vrw), Format_7_18) // [1, 2, 3][4, 5, 6, 7, 8][9, 10, 11, 12, 13, 14, 15]

	diff := accumulateDiffSplices(l2, l1)

	expected := []Splice{
		{3, 0, 5, 3},
	}

	assert.Equal(expected, diff)
}

func TestListTypeAfterMutations(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	test := func(n int, c interface{}) {
		vrw := newTestValueStore()
		values := generateNumbersAsValues(n)

		// TODO(binformat)
		l := NewList(context.Background(), Format_7_18, vrw, values...)
		assert.Equal(l.Len(), uint64(n))
		assert.IsType(c, l.asSequence())
		assert.True(TypeOf(l).Equals(MakeListType(FloaTType)))

		l = l.Edit().Append(String("a")).List(context.Background())
		assert.Equal(l.Len(), uint64(n+1))
		assert.IsType(c, l.asSequence())
		assert.True(TypeOf(l).Equals(MakeListType(MakeUnionType(FloaTType, StringType))))

		l = l.Edit().Splice(l.Len()-1, 1).List(context.Background())
		assert.Equal(l.Len(), uint64(n))
		assert.IsType(c, l.asSequence())
		assert.True(TypeOf(l).Equals(MakeListType(FloaTType)))
	}

	test(15, listLeafSequence{})
	test(1500, metaSequence{})
}

func TestListRemoveLastWhenNotLoaded(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()
	reload := func(l List) List {
		return vs.ReadValue(context.Background(), vs.WriteValue(context.Background(), l).TargetHash()).(List)
	}

	tl := newTestList(1024)
	nl := tl.toList(vs)

	for len(tl) > 0 {
		tl = tl[:len(tl)-1]
		nl = reload(nl.Edit().RemoveAt(uint64(len(tl))).List(context.Background()))
		assert.True(tl.toList(vs).Equals(nl))
	}
}

func TestListConcat(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()
	reload := func(vs *ValueStore, l List) List {
		return vs.ReadValue(context.Background(), vs.WriteValue(context.Background(), l).TargetHash()).(List)
	}

	run := func(seed int64, size, from, to, by int) {
		r := rand.New(rand.NewSource(seed))

		listSlice := make(testList, size)
		for i := range listSlice {
			listSlice[i] = Float(r.Intn(size))
		}

		list := listSlice.toList(vs)

		for i := from; i < to; i += by {
			fst := reload(vs, listSlice[:i].toList(vs))
			snd := reload(vs, listSlice[i:].toList(vs))
			actual := fst.Concat(context.Background(), snd)
			assert.True(list.Equals(actual),
				"fail at %d/%d (with expected length %d, actual %d)", i, size, list.Len(), actual.Len())
		}
	}

	run(0, 10, 0, 10, 1)

	run(1, 100, 0, 100, 1)

	run(2, 1000, 0, 1000, 10)
	run(3, 1000, 0, 100, 1)
	run(4, 1000, 900, 1000, 1)

	run(5, 1e4, 0, 1e4, 100)
	run(6, 1e4, 0, 1000, 10)
	run(7, 1e4, 1e4-1000, 1e4, 10)
}

func TestListConcatDifferentTypes(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	fst := generateNumbersAsValuesFromToBy(0, testListSize/2, 1)
	snd := generateNumbersAsStructsFromToBy(testListSize/2, testListSize, 1)

	var whole ValueSlice
	whole = append(whole, fst...)
	whole = append(whole, snd...)

	// TODO(binformat)
	concat := NewList(context.Background(), Format_7_18, vrw, fst...).Concat(context.Background(), NewList(context.Background(), Format_7_18, vrw, snd...))
	assert.True(NewList(context.Background(), Format_7_18, vrw, whole...).Equals(concat))
}

func TestListWithStructShouldHaveOptionalFields(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	// TODO(binformat)
	list := NewList(context.Background(), Format_7_18, vrw,
		NewStruct("Foo", StructData{
			"a": Float(1),
		}),
		NewStruct("Foo", StructData{
			"a": Float(2),
			"b": String("bar"),
		}),
	)
	assert.True(
		MakeListType(MakeStructType("Foo",
			StructField{"a", FloaTType, false},
			StructField{"b", StringType, true},
		),
		).Equals(TypeOf(list)))
}

func TestListWithNil(t *testing.T) {
	vrw := newTestValueStore()

	assert.Panics(t, func() {
		// TODO(binformat)
		NewList(context.Background(), Format_7_18, vrw, nil)
	})
	assert.Panics(t, func() {
		NewList(context.Background(), Format_7_18, vrw, Float(42), nil)
	})
}

func TestListOfListsDoesNotWriteRoots(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	// TODO(binformat)
	l1 := NewList(context.Background(), Format_7_18, vrw, String("a"), String("b"))
	l2 := NewList(context.Background(), Format_7_18, vrw, String("c"), String("d"))
	l3 := NewList(context.Background(), Format_7_18, vrw, l1, l2)

	// TODO(binformat)
	assert.Nil(vrw.ReadValue(context.Background(), l1.Hash(Format_7_18)))
	assert.Nil(vrw.ReadValue(context.Background(), l2.Hash(Format_7_18)))
	assert.Nil(vrw.ReadValue(context.Background(), l3.Hash(Format_7_18)))

	vrw.WriteValue(context.Background(), l3)
	// TODO(binformat)
	assert.Nil(vrw.ReadValue(context.Background(), l1.Hash(Format_7_18)))
	assert.Nil(vrw.ReadValue(context.Background(), l2.Hash(Format_7_18)))
}
