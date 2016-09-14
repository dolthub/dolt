// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"math/rand"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
)

const testListSize = 5000

type testList ValueSlice

func (tl testList) Set(idx int, v Value) (res testList) {
	res = append(res, tl[:idx]...)
	res = append(res, v)
	res = append(res, tl[idx+1:]...)
	return
}

func (tl testList) Insert(idx int, vs ...Value) (res testList) {
	res = append(res, tl[:idx]...)
	res = append(res, vs...)
	res = append(res, tl[idx:]...)
	return
}

func (tl testList) Remove(start, end int) (res testList) {
	res = append(res, tl[:start]...)
	res = append(res, tl[end:]...)
	return
}

func (tl testList) RemoveAt(idx int) testList {
	return tl.Remove(idx, idx+1)
}

func (tl testList) Diff(last testList) []Splice {
	// Note: this could be use tl.toList/last.toList and then tlList.Diff(lastList)
	// but the purpose of this method is to be redundant.
	return calcSplices(uint64(len(last)), uint64(len(tl)), DEFAULT_MAX_SPLICE_MATRIX_SIZE,
		func(i uint64, j uint64) bool { return last[i] == tl[j] })
}

func (tl testList) toList() List {
	return NewList(tl...)
}

func newTestList(length int) testList {
	return generateNumbersAsValues(length)
}

func newTestListFromList(list List) testList {
	tl := testList{}
	list.IterAll(func(v Value, idx uint64) {
		tl = append(tl, v)
	})
	return tl
}

func validateList(t *testing.T, l List, values ValueSlice) {
	assert.True(t, l.Equals(NewList(values...)))
	out := ValueSlice{}
	l.IterAll(func(v Value, idx uint64) {
		out = append(out, v)
	})
	assert.True(t, out.Equals(values))
}

type listTestSuite struct {
	collectionTestSuite
	elems testList
}

func newListTestSuite(size uint, expectRefStr string, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int) *listTestSuite {
	length := 1 << size
	elems := newTestList(length)
	tr := MakeListType(NumberType)
	list := NewList(elems...)
	return &listTestSuite{
		collectionTestSuite: collectionTestSuite{
			col:                    list,
			expectType:             tr,
			expectLen:              uint64(length),
			expectRef:              expectRefStr,
			expectChunkCount:       expectChunkCount,
			expectPrependChunkDiff: expectPrependChunkDiff,
			expectAppendChunkDiff:  expectAppendChunkDiff,
			validate: func(v2 Collection) bool {
				l2 := v2.(List)
				out := ValueSlice{}
				l2.IterAll(func(v Value, index uint64) {
					out = append(out, v)
				})
				return ValueSlice(elems).Equals(out)
			},
			prependOne: func() Collection {
				dup := make([]Value, length+1)
				dup[0] = Number(0)
				copy(dup[1:], elems)
				return NewList(dup...)
			},
			appendOne: func() Collection {
				dup := make([]Value, length+1)
				copy(dup, elems)
				dup[len(dup)-1] = Number(0)
				return NewList(dup...)
			},
		},
		elems: elems,
	}
}

func (suite *listTestSuite) TestGet() {
	list := suite.col.(List)
	for i := 0; i < len(suite.elems); i++ {
		suite.True(suite.elems[i].Equals(list.Get(uint64(i))))
	}
	suite.Equal(suite.expectLen, list.Len())
}

func (suite *listTestSuite) TestIter() {
	list := suite.col.(List)
	expectIdx := uint64(0)
	endAt := suite.expectLen / 2
	list.Iter(func(v Value, idx uint64) bool {
		suite.Equal(expectIdx, idx)
		expectIdx++
		suite.Equal(suite.elems[idx], v)
		return expectIdx == endAt
	})

	suite.Equal(endAt, expectIdx)
}

func (suite *listTestSuite) TestMap() {
	list := suite.col.(List)
	l := list.Map(func(v Value, i uint64) interface{} {
		v1 := v.(Number)
		return v1 + Number(i)
	})

	suite.Equal(uint64(len(l)), suite.expectLen)
	for i := 0; i < len(l); i++ {
		suite.Equal(l[i], list.Get(uint64(i)).(Number)+Number(i))
	}
}

func TestListSuite1K(t *testing.T) {
	suite.Run(t, newListTestSuite(10, "1md2squldk4fo7sg179pbqvdd6a3aa4p", 0, 0, 0))
}

func TestListSuite4K(t *testing.T) {
	suite.Run(t, newListTestSuite(12, "8h3s3pjmp2ihbr7270iqe446ij3bfmqr", 2, 2, 2))
}

func TestListSuite8K(t *testing.T) {
	suite.Run(t, newListTestSuite(14, "v936b655mg56lb9jh7951ielec80et15", 5, 2, 2))
}

func TestListInsert(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	tl := newTestList(1024)
	list := tl.toList()

	for i := 0; i < len(tl); i += 16 {
		tl = tl.Insert(i, Number(i))
		list = list.Insert(uint64(i), Number(i))
	}

	assert.True(tl.toList().Equals(list))
}

func TestListRemove(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	tl := newTestList(1024)
	list := tl.toList()

	for i := len(tl) - 16; i >= 0; i -= 16 {
		tl = tl.Remove(i, i+4)
		list = list.Remove(uint64(i), uint64(i+4))
	}

	assert.True(tl.toList().Equals(list))
}

func TestListRemoveAt(t *testing.T) {
	assert := assert.New(t)

	l0 := NewList()
	l0 = l0.Append(Bool(false), Bool(true))
	l1 := l0.RemoveAt(1)
	assert.True(NewList(Bool(false)).Equals(l1))
	l1 = l1.RemoveAt(0)
	assert.True(NewList().Equals(l1))

	assert.Panics(func() {
		l1.RemoveAt(0)
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
		values[i] = Number(s.Int63() & 0xff)
	}

	return values
}

func getTestListUnique() testList {
	length := int(getTestListLen())
	s := rand.NewSource(42)
	uniques := map[int64]bool{}
	for len(uniques) < length {
		uniques[s.Int63()] = true
	}
	values := make([]Value, 0, length)
	for k := range uniques {
		values = append(values, Number(k))
	}
	return values
}

func testListFromNomsList(list List) testList {
	simple := make(testList, list.Len())
	list.IterAll(func(v Value, offset uint64) {
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

	vs := NewTestValueStore()
	simpleList := getTestList()

	cl := NewList(simpleList...)
	valueChan := make(chan Value)
	listChan := NewStreamingList(vs, valueChan)
	for _, v := range simpleList {
		valueChan <- v
	}
	close(valueChan)
	sl := <-listChan
	assert.True(cl.Equals(sl))
	cl.Iter(func(v Value, idx uint64) (done bool) {
		done = !assert.True(v.Equals(sl.Get(idx)))
		return
	})
}

func TestListAppend(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	newList := func(items testList) List {
		return NewList(items...)
	}

	listToSimple := func(cl List) (simple testList) {
		cl.IterAll(func(v Value, offset uint64) {
			simple = append(simple, v)
		})
		return
	}

	cl := newList(getTestList())
	cl2 := cl.Append(Number(42))
	cl3 := cl2.Append(Number(43))
	cl4 := cl3.Append(getTestList()...)
	cl5 := cl4.Append(Number(44), Number(45))
	cl6 := cl5.Append(getTestList()...)

	expected := getTestList()
	assert.Equal(expected, listToSimple(cl))
	assert.Equal(getTestListLen(), cl.Len())
	assert.True(newList(expected).Equals(cl))

	expected = append(expected, Number(42))
	assert.Equal(expected, listToSimple(cl2))
	assert.Equal(getTestListLen()+1, cl2.Len())
	assert.True(newList(expected).Equals(cl2))

	expected = append(expected, Number(43))
	assert.Equal(expected, listToSimple(cl3))
	assert.Equal(getTestListLen()+2, cl3.Len())
	assert.True(newList(expected).Equals(cl3))

	expected = append(expected, getTestList()...)
	assert.Equal(expected, listToSimple(cl4))
	assert.Equal(2*getTestListLen()+2, cl4.Len())
	assert.True(newList(expected).Equals(cl4))

	expected = append(expected, Number(44), Number(45))
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

	values := generateNumbersAsValues(1000)

	s := NewList()
	for i, v := range values {
		s = s.Insert(uint64(i), v)
		validateList(t, s, values[0:i+1])
	}
}

func TestListValidateInsertAtZero(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	values := generateNumbersAsValues(1000)
	s := NewList()
	count := len(values)
	for count > 0 {
		count--
		v := values[count]
		s = s.Insert(uint64(0), v)
		validateList(t, s, values[count:])
	}
}

func TestListInsertNothing(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	cl := getTestList().toList()

	assert.True(cl.Equals(cl.Insert(0)))
	for i := uint64(1); i < getTestListLen(); i *= 2 {
		assert.True(cl.Equals(cl.Insert(i)))
	}
	assert.True(cl.Equals(cl.Insert(cl.Len() - 1)))
	assert.True(cl.Equals(cl.Insert(cl.Len())))
}

func TestListInsertStart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	cl := getTestList().toList()
	cl2 := cl.Insert(0, Number(42))
	cl3 := cl2.Insert(0, Number(43))
	cl4 := cl3.Insert(0, getTestList()...)
	cl5 := cl4.Insert(0, Number(44), Number(45))
	cl6 := cl5.Insert(0, getTestList()...)

	expected := getTestList()
	assert.Equal(expected, testListFromNomsList(cl))
	assert.Equal(getTestListLen(), cl.Len())
	assert.True(expected.toList().Equals(cl))

	expected = expected.Insert(0, Number(42))
	assert.Equal(expected, testListFromNomsList(cl2))
	assert.Equal(getTestListLen()+1, cl2.Len())
	assert.True(expected.toList().Equals(cl2))

	expected = expected.Insert(0, Number(43))
	assert.Equal(expected, testListFromNomsList(cl3))
	assert.Equal(getTestListLen()+2, cl3.Len())
	assert.True(expected.toList().Equals(cl3))

	expected = expected.Insert(0, getTestList()...)
	assert.Equal(expected, testListFromNomsList(cl4))
	assert.Equal(2*getTestListLen()+2, cl4.Len())
	assert.True(expected.toList().Equals(cl4))

	expected = expected.Insert(0, Number(44), Number(45))
	assert.Equal(expected, testListFromNomsList(cl5))
	assert.Equal(2*getTestListLen()+4, cl5.Len())
	assert.True(expected.toList().Equals(cl5))

	expected = expected.Insert(0, getTestList()...)
	assert.Equal(expected, testListFromNomsList(cl6))
	assert.Equal(3*getTestListLen()+4, cl6.Len())
	assert.True(expected.toList().Equals(cl6))
}

func TestListInsertMiddle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	cl := getTestList().toList()
	cl2 := cl.Insert(100, Number(42))
	cl3 := cl2.Insert(200, Number(43))
	cl4 := cl3.Insert(300, getTestList()...)
	cl5 := cl4.Insert(400, Number(44), Number(45))
	cl6 := cl5.Insert(500, getTestList()...)
	cl7 := cl6.Insert(600, Number(100))

	expected := getTestList()
	assert.Equal(expected, testListFromNomsList(cl))
	assert.Equal(getTestListLen(), cl.Len())
	assert.True(expected.toList().Equals(cl))

	expected = expected.Insert(100, Number(42))
	assert.Equal(expected, testListFromNomsList(cl2))
	assert.Equal(getTestListLen()+1, cl2.Len())
	assert.True(expected.toList().Equals(cl2))

	expected = expected.Insert(200, Number(43))
	assert.Equal(expected, testListFromNomsList(cl3))
	assert.Equal(getTestListLen()+2, cl3.Len())
	assert.True(expected.toList().Equals(cl3))

	expected = expected.Insert(300, getTestList()...)
	assert.Equal(expected, testListFromNomsList(cl4))
	assert.Equal(2*getTestListLen()+2, cl4.Len())
	assert.True(expected.toList().Equals(cl4))

	expected = expected.Insert(400, Number(44), Number(45))
	assert.Equal(expected, testListFromNomsList(cl5))
	assert.Equal(2*getTestListLen()+4, cl5.Len())
	assert.True(expected.toList().Equals(cl5))

	expected = expected.Insert(500, getTestList()...)
	assert.Equal(expected, testListFromNomsList(cl6))
	assert.Equal(3*getTestListLen()+4, cl6.Len())
	assert.True(expected.toList().Equals(cl6))

	expected = expected.Insert(600, Number(100))
	assert.Equal(expected, testListFromNomsList(cl7))
	assert.Equal(3*getTestListLen()+5, cl7.Len())
	assert.True(expected.toList().Equals(cl7))
}

func TestListInsertRanges(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	testList := getTestList()
	whole := testList.toList()

	// Compare list equality. Increment by 256 (16^2) because each iteration requires building a new list, which is slow.
	for incr, i := 256, 0; i < len(testList)-incr; i += incr {
		for window := 1; window <= incr; window *= 16 {
			testListPart := testList.Remove(i, i+window)
			actual := testListPart.toList().Insert(uint64(i), testList[i:i+window]...)
			assert.Equal(whole.Len(), actual.Len())
			assert.True(whole.Equals(actual))
		}
	}

	// Compare list length, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList); i += incr {
		assert.Equal(len(testList)+incr, int(whole.Insert(uint64(i), testList[0:incr]...).Len()))
	}
}

func TestListRemoveNothing(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	cl := getTestList().toList()

	assert.True(cl.Equals(cl.Remove(0, 0)))
	for i := uint64(1); i < getTestListLen(); i *= 2 {
		assert.True(cl.Equals(cl.Remove(i, i)))
	}
	assert.True(cl.Equals(cl.Remove(cl.Len()-1, cl.Len()-1)))
	assert.True(cl.Equals(cl.Remove(cl.Len(), cl.Len())))
}

func TestListRemoveEverything(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	cl := getTestList().toList().Remove(0, getTestListLen())

	assert.True(NewList().Equals(cl))
	assert.Equal(0, int(cl.Len()))
}

func TestListRemoveAtMiddle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	cl := getTestList().toList()
	cl2 := cl.RemoveAt(100)
	cl3 := cl2.RemoveAt(200)

	expected := getTestList()
	assert.Equal(expected, testListFromNomsList(cl))
	assert.Equal(getTestListLen(), cl.Len())
	assert.True(expected.toList().Equals(cl))

	expected = expected.RemoveAt(100)
	assert.Equal(expected, testListFromNomsList(cl2))
	assert.Equal(getTestListLen()-1, cl2.Len())
	assert.True(expected.toList().Equals(cl2))

	expected = expected.RemoveAt(200)
	assert.Equal(expected, testListFromNomsList(cl3))
	assert.Equal(getTestListLen()-2, cl3.Len())
	assert.True(expected.toList().Equals(cl3))
}

func TestListRemoveRanges(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	testList := getTestList()
	whole := testList.toList()

	// Compare list equality. Increment by 256 (16^2) because each iteration requires building a new list, which is slow.
	for incr, i := 256, 0; i < len(testList)-incr; i += incr {
		for window := 1; window <= incr; window *= 16 {
			testListPart := testList.Remove(i, i+window)
			expected := testListPart.toList()
			actual := whole.Remove(uint64(i), uint64(i+window))
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
		}
	}

	// Compare list length, which doesn't require building a new list every iteration, so the increment can be smaller.
	for incr, i := 10, 0; i < len(testList)-incr; i += incr {
		assert.Equal(len(testList)-incr, int(whole.Remove(uint64(i), uint64(i+incr)).Len()))
	}
}

func TestListRemoveAtEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	tl := getTestListWithLen(testListSize / 10)
	cl := tl.toList()

	for i := len(tl) - 1; i >= 0; i-- {
		cl = cl.Remove(uint64(i), uint64(i+1))
		expect := tl[0:i].toList()
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

	testList := getTestList()
	cl := testList.toList()

	testIdx := func(idx int, testEquality bool) {
		newVal := Number(-1) // Test values are never < 0
		cl2 := cl.Set(uint64(idx), newVal)
		assert.False(cl.Equals(cl2))
		if testEquality {
			assert.True(testList.Set(idx, newVal).toList().Equals(cl2))
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
	assert := assert.New(t)

	nums := generateNumbersAsValues(testListSize)
	s := NewList(nums...)
	assert.Equal("tqpbqlu036sosdq9kg3lka7sjaklgslg", s.Hash().String())
}

func TestListRefOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	nums := generateNumbersAsRefOfStructs(testListSize)
	s := NewList(nums...)
	assert.Equal("6l8ivdkncvks19rsmtempkoklc3s1n2q", s.Hash().String())
}

func TestListModifyAfterRead(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := NewTestValueStore()

	list := getTestList().toList()
	// Drop chunk values.
	list = vs.ReadValue(vs.WriteValue(list).TargetHash()).(List)
	// Modify/query. Once upon a time this would crash.
	llen := list.Len()
	z := list.Get(0)
	list = list.RemoveAt(0)
	assert.Equal(llen-1, list.Len())
	list = list.Append(z)
	assert.Equal(llen, list.Len())
}

func accumulateDiffSplices(l1, l2 List) (diff []Splice) {
	diffChan := make(chan Splice)
	go func() {
		l1.Diff(l2, diffChan, nil)
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
		l1.DiffWithLimit(l2, diffChan, nil, maxSpliceMatrixSize)
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

	assert := assert.New(t)
	nums := generateNumbersAsValues(5)
	l1 := NewList(nums...)
	l2 := NewList(nums...)

	diff1 := accumulateDiffSplices(l1, l2)
	diff2 := accumulateDiffSplices(l2, l1)

	assert.Equal(0, len(diff1))
	assert.Equal(0, len(diff2))
}

func TestListDiffVersusEmpty(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5)
	l1 := NewList(nums1...)
	l2 := NewList()

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

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5000)
	nums2 := reverseValues(nums1)
	l1 := NewList(nums1...)
	l2 := NewList(nums2...)

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

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5000)
	nums2 := reverseValues(nums1)
	l1 := NewList(nums1...)
	l2 := NewList(nums2...)

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

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5000)
	nums2 := generateNumbersAsValues(5000)
	for count := 5; count > 0; count-- {
		nums2 = spliceValues(nums2, (count-1)*1000, 100)
	}
	l1 := NewList(nums1...)
	l2 := NewList(nums2...)

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

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5000)
	nums2 := generateNumbersAsValues(5000)
	for count := 5; count > 0; count-- {
		nums2 = spliceValues(nums2, (count-1)*1000, 0, Number(0), Number(1), Number(2), Number(3), Number(4))
	}
	l1 := NewList(nums1...)
	l2 := NewList(nums2...)

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

	assert := assert.New(t)
	nums1 := generateNumbersAsValues(5000)
	nums2 := generateNumbersAsValues(5000)
	for count := 5; count > 0; count-- {
		out := reverseValues(nums2[(count-1)*1000 : (count-1)*1000+100])
		nums2 = spliceValues(nums2, (count-1)*1000, 100, out...)
	}
	l1 := NewList(nums1...)
	l2 := NewList(nums2...)
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

	assert := assert.New(t)
	nums1 := []Value{String("one"), String("two"), String("three")}
	nums2 := []Value{String("one"), String("two"), String("three")}
	l1 := NewList(nums1...)
	l2 := NewList(nums2...)
	diff := accumulateDiffSplices(l2, l1)

	assert.Equal(0, len(diff))
}

func TestListDiffString2(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)
	nums1 := []Value{String("one"), String("two"), String("three")}
	nums2 := []Value{String("one"), String("two"), String("three"), String("four")}
	l1 := NewList(nums1...)
	l2 := NewList(nums2...)
	diff := accumulateDiffSplices(l2, l1)

	diffExpected := []Splice{
		{3, 0, 1, 3},
	}
	assert.Equal(diffExpected, diff, "expected diff is wrong")
}

func TestListDiffString3(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)
	nums1 := []Value{String("one"), String("two"), String("three")}
	nums2 := []Value{String("one"), String("two"), String("four")}
	l1 := NewList(nums1...)
	l2 := NewList(nums2...)
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

	cs1 := chunks.NewTestStore()
	vs1 := newLocalValueStore(cs1)
	nums1 := generateNumbersAsValues(4000)
	l1 := NewList(nums1...)
	ref1 := vs1.WriteValue(l1).TargetHash()
	refList1 := vs1.ReadValue(ref1).(List)

	cs2 := chunks.NewTestStore()
	vs2 := newLocalValueStore(cs2)
	nums2 := generateNumbersAsValuesFromToBy(5, 3550, 1)
	l2 := NewList(nums2...)
	ref2 := vs2.WriteValue(l2).TargetHash()
	refList2 := vs2.ReadValue(ref2).(List)

	// diff lists without value store
	diff1 := accumulateDiffSplices(l2, l1)
	assert.Equal(2, len(diff1))

	// diff lists from value stores
	diff2 := accumulateDiffSplices(refList2, refList1)
	assert.Equal(2, len(diff2))

	// diff without and with value store should be same
	assert.Equal(diff1, diff2)

	// should only read/write a "small & reasonably sized portion of the total"
	assert.Equal(3, cs1.Writes)
	assert.Equal(3, cs1.Reads)
	assert.Equal(3, cs2.Writes)
	assert.Equal(3, cs2.Reads)
}

func TestListDiffAllValuesInSequenceRemoved(t *testing.T) {
	assert := assert.New(t)

	newSequenceMetaTuple := func(vs ...Value) metaTuple {
		seq := newListLeafSequence(nil, vs...)
		list := newList(seq)
		return newMetaTuple(NewRef(list), orderedKeyFromInt(len(vs)), uint64(len(vs)), list)
	}

	m1 := newSequenceMetaTuple(Number(1), Number(2), Number(3))
	m2 := newSequenceMetaTuple(Number(4), Number(5), Number(6), Number(7), Number(8))
	m3 := newSequenceMetaTuple(Number(9), Number(10), Number(11), Number(12), Number(13), Number(14), Number(15))

	l1 := newList(newListMetaSequence([]metaTuple{m1, m3}, nil))     // [1, 2, 3][9, 10, 11, 12, 13, 14, 15]
	l2 := newList(newListMetaSequence([]metaTuple{m1, m2, m3}, nil)) // [1, 2, 3][4, 5, 6, 7, 8][9, 10, 11, 12, 13, 14, 15]

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
		values := generateNumbersAsValues(n)

		l := NewList(values...)
		assert.Equal(l.Len(), uint64(n))
		assert.IsType(c, l.sequence())
		assert.True(l.Type().Equals(MakeListType(NumberType)))

		l = l.Append(String("a"))
		assert.Equal(l.Len(), uint64(n+1))
		assert.IsType(c, l.sequence())
		assert.True(l.Type().Equals(MakeListType(MakeUnionType(NumberType, StringType))))

		l = l.Splice(l.Len()-1, 1)
		assert.Equal(l.Len(), uint64(n))
		assert.IsType(c, l.sequence())
		assert.True(l.Type().Equals(MakeListType(NumberType)))
	}

	test(15, listLeafSequence{})
	test(1500, indexedMetaSequence{})
}

func TestListRemoveLastWhenNotLoaded(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := NewTestValueStore()
	reload := func(l List) List {
		return vs.ReadValue(vs.WriteValue(l).TargetHash()).(List)
	}

	tl := newTestList(1024)
	nl := tl.toList()

	for len(tl) > 0 {
		tl = tl[:len(tl)-1]
		nl = reload(nl.RemoveAt(uint64(len(tl))))
		assert.True(tl.toList().Equals(nl))
	}
}

func TestListConcat(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := NewTestValueStore()
	reload := func(vs *ValueStore, l List) List {
		return vs.ReadValue(vs.WriteValue(l).TargetHash()).(List)
	}

	run := func(seed int64, size, from, to, by int) {
		r := rand.New(rand.NewSource(seed))

		listSlice := make(testList, size)
		for i := range listSlice {
			listSlice[i] = Number(r.Intn(size))
		}

		list := listSlice.toList()

		for i := from; i < to; i += by {
			fst := reload(vs, listSlice[:i].toList())
			snd := reload(vs, listSlice[i:].toList())
			actual := fst.Concat(snd)
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
