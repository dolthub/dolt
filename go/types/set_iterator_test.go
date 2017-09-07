// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numbers := append(generateNumbersAsValues(10), Number(20), Number(25))
	s := NewSet(vs, numbers...)
	i := s.Iterator()
	vals := iterToSlice(i)
	assert.True(vals.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i = s.Iterator()
	assert.Panics(func() { i.SkipTo(nil) })
	assert.Equal(Number(0), i.SkipTo(Number(-20)))
	assert.Equal(Number(2), i.SkipTo(Number(2)))
	assert.Equal(Number(3), i.SkipTo(Number(-20)))
	assert.Equal(Number(5), i.SkipTo(Number(5)))
	assert.Equal(Number(6), i.Next())
	assert.Equal(Number(7), i.SkipTo(Number(6)))
	assert.Equal(Number(20), i.SkipTo(Number(15)))
	assert.Nil(i.SkipTo(Number(30)))
	assert.Nil(i.SkipTo(Number(30)))
	assert.Nil(i.SkipTo(Number(1)))

	i = s.Iterator()
	assert.Equal(Number(0), i.Next())
	assert.Equal(Number(1), i.Next())
	assert.Equal(Number(3), i.SkipTo(Number(3)))
	assert.Equal(Number(4), i.Next())

	empty := NewSet(vs)
	assert.Nil(empty.Iterator().Next())
	assert.Nil(empty.Iterator().SkipTo(Number(-30)))

	single := NewSet(vs, Number(42)).Iterator()
	assert.Equal(Number(42), single.SkipTo(Number(42)))
	assert.Equal(nil, single.SkipTo(Number(42)))

	single = NewSet(vs, Number(42)).Iterator()
	assert.Equal(Number(42), single.SkipTo(Number(42)))
	assert.Equal(nil, single.Next())

	single = NewSet(vs, Number(42)).Iterator()
	assert.Equal(Number(42), single.SkipTo(Number(21)))
}

func TestSetIteratorAt(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numbers := append(generateNumbersAsValues(5), Number(10))
	s := NewSet(vs, numbers...)
	i := s.IteratorAt(0)
	vals := iterToSlice(i)
	assert.True(vals.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i = s.IteratorAt(2)
	vals = iterToSlice(i)
	assert.True(vals.Equals(numbers[2:]), "Expected: %v != actual: %v", numbers[2:], vs)

	i = s.IteratorAt(10)
	vals = iterToSlice(i)
	assert.True(vals.Equals(nil), "Expected: %v != actual: %v", nil, vs)
}

func TestSetIteratorFrom(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numbers := append(generateNumbersAsValues(5), Number(10), Number(20))
	s := NewSet(vs, numbers...)
	i := s.IteratorFrom(Number(0))
	vals := iterToSlice(i)
	assert.True(vals.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i = s.IteratorFrom(Number(2))
	vals = iterToSlice(i)
	assert.True(vals.Equals(numbers[2:]), "Expected: %v != actual: %v", numbers[2:], vs)

	i = s.IteratorFrom(Number(10))
	vals = iterToSlice(i)
	assert.True(vals.Equals(ValueSlice{Number(10), Number(20)}), "Expected: %v != actual: %v", nil, vs)

	i = s.IteratorFrom(Number(20))
	vals = iterToSlice(i)
	assert.True(vals.Equals(ValueSlice{Number(20)}), "Expected: %v != actual: %v", nil, vs)

	i = s.IteratorFrom(Number(100))
	vals = iterToSlice(i)
	assert.True(vals.Equals(nil), "Expected: %v != actual: %v", nil, vs)

	// Not present. Starts at next larger.
	i = s.IteratorFrom(Number(15))
	vals = iterToSlice(i)
	assert.True(vals.Equals(ValueSlice{Number(20)}), "Expected: %v != actual: %v", nil, vs)
}

func TestUnionIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	set1 := NewSet(vs, generateNumbersAsValuesFromToBy(0, 10, 1)...)
	set2 := NewSet(vs, generateNumbersAsValuesFromToBy(5, 15, 1)...)
	set3 := NewSet(vs, generateNumbersAsValuesFromToBy(10, 20, 1)...)
	set4 := NewSet(vs, generateNumbersAsValuesFromToBy(15, 25, 1)...)

	ui1 := NewUnionIterator(set1.Iterator(), set2.Iterator())
	vals := iterToSlice(ui1)
	expectedRes := generateNumbersAsValues(15)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	ui1 = NewUnionIterator(set1.Iterator(), set4.Iterator())
	ui2 := NewUnionIterator(set3.Iterator(), set2.Iterator())
	ui3 := NewUnionIterator(ui1, ui2)
	vals = iterToSlice(ui3)
	expectedRes = generateNumbersAsValues(25)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	ui1 = NewUnionIterator(set1.Iterator(), set4.Iterator())
	ui2 = NewUnionIterator(set3.Iterator(), set2.Iterator())
	ui3 = NewUnionIterator(ui1, ui2)

	assert.Panics(func() { ui3.SkipTo(nil) })
	assert.Equal(Number(0), ui3.SkipTo(Number(-5)))
	assert.Equal(Number(5), ui3.SkipTo(Number(5)))
	assert.Equal(Number(8), ui3.SkipTo(Number(8)))
	assert.Equal(Number(9), ui3.SkipTo(Number(8)))
	assert.Equal(Number(10), ui3.SkipTo(Number(8)))
	assert.Equal(Number(11), ui3.SkipTo(Number(7)))
	assert.Equal(Number(12), ui3.Next())
	assert.Equal(Number(15), ui3.SkipTo(Number(15)))
	assert.Equal(Number(24), ui3.SkipTo(Number(24)))
	assert.Nil(ui3.SkipTo(Number(25)))

	singleElemSet := NewSet(vs, Number(4))
	emptySet := NewSet(vs)

	ui10 := NewUnionIterator(singleElemSet.Iterator(), singleElemSet.Iterator())
	ui20 := NewUnionIterator(emptySet.Iterator(), emptySet.Iterator())
	ui30 := NewUnionIterator(ui10, ui20)
	vals = iterToSlice(ui30)
	expectedRes = ValueSlice{Number(4)}
	assert.True(vals.Equals(expectedRes), "%v != %v\n", expectedRes, vs)
}

func TestIntersectionIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	byTwos := NewSet(vs, generateNumbersAsValuesFromToBy(0, 200, 2)...)
	byThrees := NewSet(vs, generateNumbersAsValuesFromToBy(0, 200, 3)...)
	byFives := NewSet(vs, generateNumbersAsValuesFromToBy(0, 200, 5)...)

	i1 := NewIntersectionIterator(byTwos.Iterator(), byThrees.Iterator())
	vals := iterToSlice(i1)
	expectedRes := generateNumbersAsValuesFromToBy(0, 200, 6)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	it1 := NewIntersectionIterator(byTwos.Iterator(), byThrees.Iterator())
	it2 := NewIntersectionIterator(it1, byFives.Iterator())
	vals = iterToSlice(it2)
	expectedRes = generateNumbersAsValuesFromToBy(0, 200, 30)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	it1 = NewIntersectionIterator(byThrees.Iterator(), byFives.Iterator())
	it2 = NewIntersectionIterator(it1, byTwos.Iterator())

	assert.Panics(func() { it2.SkipTo(nil) })
	assert.Equal(Number(30), it2.SkipTo(Number(5)))
	assert.Equal(Number(60), it2.SkipTo(Number(60)))
	assert.Equal(Number(90), it2.SkipTo(Number(5)))
	assert.Equal(Number(120), it2.Next())
	assert.Equal(Number(150), it2.SkipTo(Number(150)))
	assert.Nil(it2.SkipTo(Number(40000)))
}

func TestCombinationIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	byTwos := NewSet(vs, generateNumbersAsValuesFromToBy(0, 70, 2)...)
	byThrees := NewSet(vs, generateNumbersAsValuesFromToBy(0, 70, 3)...)
	byFives := NewSet(vs, generateNumbersAsValuesFromToBy(0, 70, 5)...)
	bySevens := NewSet(vs, generateNumbersAsValuesFromToBy(0, 70, 7)...)

	it1 := NewIntersectionIterator(byTwos.Iterator(), bySevens.Iterator())
	it2 := NewIntersectionIterator(byFives.Iterator(), byThrees.Iterator())
	ut1 := NewUnionIterator(it1, it2)
	vals := iterToSlice(ut1)
	expectedRes := intsToValueSlice(0, 14, 15, 28, 30, 42, 45, 56, 60)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	ut1 = NewUnionIterator(byTwos.Iterator(), bySevens.Iterator())
	it2 = NewIntersectionIterator(byFives.Iterator(), byThrees.Iterator())
	ut2 := NewIntersectionIterator(ut1, it2)
	vals = iterToSlice(ut2)
	expectedRes = intsToValueSlice(0, 30, 60)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)
}

type UnionTestIterator struct {
	*UnionIterator
	cntr *int
}

func (ui *UnionTestIterator) Next() Value {
	*ui.cntr++
	return ui.UnionIterator.Next()
}

func (ui *UnionTestIterator) SkipTo(v Value) Value {
	*ui.cntr++
	return ui.UnionIterator.SkipTo(v)
}

func NewUnionTestIterator(i1, i2 SetIterator, cntr *int) SetIterator {
	ui := NewUnionIterator(i1, i2).(*UnionIterator)
	return &UnionTestIterator{ui, cntr}
}

// When a binary tree of union operators is built on top of a list of sets, the complexity to
// retrieve all of the elements in sorted order should be Log(N) * M where N is the number of sets func init() {
// the list and M is the total number of elements in all of the sets.
func TestUnionComplexity(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numSets := 256
	numElemsPerSet := 1000
	logNumSets := int(math.Ceil(math.Log2(float64(numSets))))
	totalElems := numSets * numElemsPerSet
	expectedMax := logNumSets*totalElems + numSets

	callCount1 := 0
	iter := iterize(createSetsWithDistinctNumbers(vs, numSets, numElemsPerSet), NewUnionTestIterator, &callCount1)
	vals := iterToSlice(iter)
	expected := generateNumbersAsValueSlice(numSets * numElemsPerSet)
	assert.True(expected.Equals(vals), "expected: %v != actual: %v", expected, vals)
	assert.True(expectedMax > callCount1, "callCount: %d exceeds expectedMax: %d", callCount1, expectedMax)

	callCount2 := 0
	iter = iterize(createSetsWithSameNumbers(vs, numSets, numElemsPerSet), NewUnionTestIterator, &callCount2)
	vals = iterToSlice(iter)
	expected = generateNumbersAsValueSlice(numElemsPerSet)
	assert.True(expected.Equals(vals), "expected: %v != actual: %v", expected, vals)
	assert.True(expectedMax > callCount2, "callCount: %d exceeds expectedMax: %d", callCount2, expectedMax)
}

type IntersectionTestIterator struct {
	*IntersectionIterator
	cntr *int
}

func (i *IntersectionTestIterator) Next() Value {
	*i.cntr++
	return i.IntersectionIterator.Next()
}

func (i *IntersectionTestIterator) SkipTo(v Value) Value {
	*i.cntr++
	return i.IntersectionIterator.SkipTo(v)
}

func NewIntersectionTestIterator(i1, i2 SetIterator, cntr *int) SetIterator {
	ui := NewIntersectionIterator(i1, i2).(*IntersectionIterator)
	return &IntersectionTestIterator{ui, cntr}
}

// When a binary tree of intersection operators is built on top of a list of sets, the complexity to
// retrieve all of the elements in sorted order should be Log(N) * M where N is the number of sets func init() {
// the list and M is the total number of elements in all of the sets.
func TestIntersectComplexity(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numSets := 256
	numElemsPerSet := 1000
	logNumSets := int(math.Ceil(math.Log2(float64(numSets))))
	totalElems := numSets * numElemsPerSet
	expectedMax := logNumSets*totalElems + numSets

	callCount1 := 0
	iter := iterize(createSetsWithDistinctNumbers(vs, numSets, numElemsPerSet), NewIntersectionTestIterator, &callCount1)
	vals := iterToSlice(iter)
	expected := ValueSlice{}
	assert.True(expected.Equals(vals), "expected: %v != actual: %v", expected, vals)
	assert.True(expectedMax > callCount1, "callCount: %d exceeds expectedMax: %d", callCount1, expectedMax)

	callCount2 := 0
	iter = iterize(createSetsWithSameNumbers(vs, numSets, numElemsPerSet), NewIntersectionTestIterator, &callCount2)
	vals = iterToSlice(iter)
	expected = generateNumbersAsValueSlice(numElemsPerSet)
	assert.True(expected.Equals(vals), "expected: %v != actual: %v", expected, vals)
	assert.True(expectedMax > callCount2, "callCount: %d exceeds expectedMax: %d", callCount2, expectedMax)
}

func createSetsWithDistinctNumbers(vrw ValueReadWriter, numSets, numElemsPerSet int) []SetIterator {
	iterSlice := []SetIterator{}

	for i := 0; i < numSets; i++ {
		vals := ValueSlice{}
		for j := 0; j < numElemsPerSet; j++ {
			vals = append(vals, Number(i+(numSets*j)))
		}
		s := NewSet(vrw, vals...)
		iterSlice = append(iterSlice, s.Iterator())
	}
	return iterSlice
}

func createSetsWithSameNumbers(vrw ValueReadWriter, numSets, numElemsPerSet int) []SetIterator {
	vs := ValueSlice{}
	for j := 0; j < numElemsPerSet; j++ {
		vs = append(vs, Number(j))
	}
	iterSlice := []SetIterator{}
	for i := 0; i < numSets; i++ {
		iterSlice = append(iterSlice, NewSet(vrw, vs...).Iterator())
	}
	return iterSlice
}

type newIterFunc func(i1, i2 SetIterator, cntr *int) SetIterator

// Iterize calls itself recursively to build a binary tree of iterators over the original set.
func iterize(iters []SetIterator, newIter newIterFunc, cntr *int) SetIterator {
	if len(iters) == 0 {
		return nil
	}
	if len(iters) <= 1 {
		return iters[0]
	}
	var iter0 SetIterator
	newIters := []SetIterator{}
	for i, iter := range iters {
		if i%2 == 0 {
			iter0 = iter
		} else {
			ni := newIter(iter0, iter, cntr)
			newIters = append(newIters, ni)
			iter0 = nil
		}
	}
	if iter0 != nil {
		newIters = append(newIters, iter0)
	}
	return iterize(newIters, newIter, cntr)
}
