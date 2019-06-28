// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numbers := append(generateNumbersAsValues(10), Float(20), Float(25))
	s := NewSet(context.Background(), Format_7_18, vs, numbers...)
	i := s.Iterator(context.Background())
	vals := iterToSlice(i)
	assert.True(vals.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i = s.Iterator(context.Background())
	assert.Panics(func() { i.SkipTo(context.Background(), nil) })
	assert.Equal(Float(0), i.SkipTo(context.Background(), Float(-20)))
	assert.Equal(Float(2), i.SkipTo(context.Background(), Float(2)))
	assert.Equal(Float(3), i.SkipTo(context.Background(), Float(-20)))
	assert.Equal(Float(5), i.SkipTo(context.Background(), Float(5)))
	assert.Equal(Float(6), i.Next(context.Background()))
	assert.Equal(Float(7), i.SkipTo(context.Background(), Float(6)))
	assert.Equal(Float(20), i.SkipTo(context.Background(), Float(15)))
	assert.Nil(i.SkipTo(context.Background(), Float(30)))
	assert.Nil(i.SkipTo(context.Background(), Float(30)))
	assert.Nil(i.SkipTo(context.Background(), Float(1)))

	i = s.Iterator(context.Background())
	assert.Equal(Float(0), i.Next(context.Background()))
	assert.Equal(Float(1), i.Next(context.Background()))
	assert.Equal(Float(3), i.SkipTo(context.Background(), Float(3)))
	assert.Equal(Float(4), i.Next(context.Background()))

	empty := NewSet(context.Background(), Format_7_18, vs)
	assert.Nil(empty.Iterator(context.Background()).Next(context.Background()))
	assert.Nil(empty.Iterator(context.Background()).SkipTo(context.Background(), Float(-30)))

	single := NewSet(context.Background(), Format_7_18, vs, Float(42)).Iterator(context.Background())
	assert.Equal(Float(42), single.SkipTo(context.Background(), Float(42)))
	assert.Equal(nil, single.SkipTo(context.Background(), Float(42)))

	single = NewSet(context.Background(), Format_7_18, vs, Float(42)).Iterator(context.Background())
	assert.Equal(Float(42), single.SkipTo(context.Background(), Float(42)))
	assert.Equal(nil, single.Next(context.Background()))

	single = NewSet(context.Background(), Format_7_18, vs, Float(42)).Iterator(context.Background())
	assert.Equal(Float(42), single.SkipTo(context.Background(), Float(21)))
}

func TestSetIteratorAt(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numbers := append(generateNumbersAsValues(5), Float(10))
	s := NewSet(context.Background(), Format_7_18, vs, numbers...)
	i := s.IteratorAt(context.Background(), 0)
	vals := iterToSlice(i)
	assert.True(vals.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i = s.IteratorAt(context.Background(), 2)
	vals = iterToSlice(i)
	assert.True(vals.Equals(numbers[2:]), "Expected: %v != actual: %v", numbers[2:], vs)

	i = s.IteratorAt(context.Background(), 10)
	vals = iterToSlice(i)
	assert.True(vals.Equals(nil), "Expected: %v != actual: %v", nil, vs)
}

func TestSetIteratorFrom(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numbers := append(generateNumbersAsValues(5), Float(10), Float(20))
	s := NewSet(context.Background(), Format_7_18, vs, numbers...)
	i := s.IteratorFrom(context.Background(), Float(0))
	vals := iterToSlice(i)
	assert.True(vals.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i = s.IteratorFrom(context.Background(), Float(2))
	vals = iterToSlice(i)
	assert.True(vals.Equals(numbers[2:]), "Expected: %v != actual: %v", numbers[2:], vs)

	i = s.IteratorFrom(context.Background(), Float(10))
	vals = iterToSlice(i)
	assert.True(vals.Equals(ValueSlice{Float(10), Float(20)}), "Expected: %v != actual: %v", nil, vs)

	i = s.IteratorFrom(context.Background(), Float(20))
	vals = iterToSlice(i)
	assert.True(vals.Equals(ValueSlice{Float(20)}), "Expected: %v != actual: %v", nil, vs)

	i = s.IteratorFrom(context.Background(), Float(100))
	vals = iterToSlice(i)
	assert.True(vals.Equals(nil), "Expected: %v != actual: %v", nil, vs)

	// Not present. Starts at next larger.
	i = s.IteratorFrom(context.Background(), Float(15))
	vals = iterToSlice(i)
	assert.True(vals.Equals(ValueSlice{Float(20)}), "Expected: %v != actual: %v", nil, vs)
}

func TestUnionIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	set1 := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(0, 10, 1)...)
	set2 := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(5, 15, 1)...)
	set3 := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(10, 20, 1)...)
	set4 := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(15, 25, 1)...)

	ui1 := NewUnionIterator(context.Background(), set1.Iterator(context.Background()), set2.Iterator(context.Background()))
	vals := iterToSlice(ui1)
	expectedRes := generateNumbersAsValues(15)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	ui1 = NewUnionIterator(context.Background(), set1.Iterator(context.Background()), set4.Iterator(context.Background()))
	ui2 := NewUnionIterator(context.Background(), set3.Iterator(context.Background()), set2.Iterator(context.Background()))
	ui3 := NewUnionIterator(context.Background(), ui1, ui2)
	vals = iterToSlice(ui3)
	expectedRes = generateNumbersAsValues(25)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	ui1 = NewUnionIterator(context.Background(), set1.Iterator(context.Background()), set4.Iterator(context.Background()))
	ui2 = NewUnionIterator(context.Background(), set3.Iterator(context.Background()), set2.Iterator(context.Background()))
	ui3 = NewUnionIterator(context.Background(), ui1, ui2)

	assert.Panics(func() { ui3.SkipTo(context.Background(), nil) })
	assert.Equal(Float(0), ui3.SkipTo(context.Background(), Float(-5)))
	assert.Equal(Float(5), ui3.SkipTo(context.Background(), Float(5)))
	assert.Equal(Float(8), ui3.SkipTo(context.Background(), Float(8)))
	assert.Equal(Float(9), ui3.SkipTo(context.Background(), Float(8)))
	assert.Equal(Float(10), ui3.SkipTo(context.Background(), Float(8)))
	assert.Equal(Float(11), ui3.SkipTo(context.Background(), Float(7)))
	assert.Equal(Float(12), ui3.Next(context.Background()))
	assert.Equal(Float(15), ui3.SkipTo(context.Background(), Float(15)))
	assert.Equal(Float(24), ui3.SkipTo(context.Background(), Float(24)))
	assert.Nil(ui3.SkipTo(context.Background(), Float(25)))

	singleElemSet := NewSet(context.Background(), Format_7_18, vs, Float(4))
	emptySet := NewSet(context.Background(), Format_7_18, vs)

	ui10 := NewUnionIterator(context.Background(), singleElemSet.Iterator(context.Background()), singleElemSet.Iterator(context.Background()))
	ui20 := NewUnionIterator(context.Background(), emptySet.Iterator(context.Background()), emptySet.Iterator(context.Background()))
	ui30 := NewUnionIterator(context.Background(), ui10, ui20)
	vals = iterToSlice(ui30)
	expectedRes = ValueSlice{Float(4)}
	assert.True(vals.Equals(expectedRes), "%v != %v\n", expectedRes, vs)
}

func TestIntersectionIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	byTwos := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(0, 200, 2)...)
	byThrees := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(0, 200, 3)...)
	byFives := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(0, 200, 5)...)

	i1 := NewIntersectionIterator(context.Background(), byTwos.Iterator(context.Background()), byThrees.Iterator(context.Background()))
	vals := iterToSlice(i1)
	expectedRes := generateNumbersAsValuesFromToBy(0, 200, 6)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	it1 := NewIntersectionIterator(context.Background(), byTwos.Iterator(context.Background()), byThrees.Iterator(context.Background()))
	it2 := NewIntersectionIterator(context.Background(), it1, byFives.Iterator(context.Background()))
	vals = iterToSlice(it2)
	expectedRes = generateNumbersAsValuesFromToBy(0, 200, 30)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	it1 = NewIntersectionIterator(context.Background(), byThrees.Iterator(context.Background()), byFives.Iterator(context.Background()))
	it2 = NewIntersectionIterator(context.Background(), it1, byTwos.Iterator(context.Background()))

	assert.Panics(func() { it2.SkipTo(context.Background(), nil) })
	assert.Equal(Float(30), it2.SkipTo(context.Background(), Float(5)))
	assert.Equal(Float(60), it2.SkipTo(context.Background(), Float(60)))
	assert.Equal(Float(90), it2.SkipTo(context.Background(), Float(5)))
	assert.Equal(Float(120), it2.Next(context.Background()))
	assert.Equal(Float(150), it2.SkipTo(context.Background(), Float(150)))
	assert.Nil(it2.SkipTo(context.Background(), Float(40000)))
}

func TestCombinationIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	byTwos := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(0, 70, 2)...)
	byThrees := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(0, 70, 3)...)
	byFives := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(0, 70, 5)...)
	bySevens := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(0, 70, 7)...)

	it1 := NewIntersectionIterator(context.Background(), byTwos.Iterator(context.Background()), bySevens.Iterator(context.Background()))
	it2 := NewIntersectionIterator(context.Background(), byFives.Iterator(context.Background()), byThrees.Iterator(context.Background()))
	ut1 := NewUnionIterator(context.Background(), it1, it2)
	vals := iterToSlice(ut1)
	expectedRes := intsToValueSlice(0, 14, 15, 28, 30, 42, 45, 56, 60)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	ut1 = NewUnionIterator(context.Background(), byTwos.Iterator(context.Background()), bySevens.Iterator(context.Background()))
	it2 = NewIntersectionIterator(context.Background(), byFives.Iterator(context.Background()), byThrees.Iterator(context.Background()))
	ut2 := NewIntersectionIterator(context.Background(), ut1, it2)
	vals = iterToSlice(ut2)
	expectedRes = intsToValueSlice(0, 30, 60)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)
}

type UnionTestIterator struct {
	*UnionIterator
	cntr *int
}

func (ui *UnionTestIterator) Next(ctx context.Context) Value {
	*ui.cntr++
	return ui.UnionIterator.Next(ctx)
}

func (ui *UnionTestIterator) SkipTo(ctx context.Context, v Value) Value {
	*ui.cntr++
	return ui.UnionIterator.SkipTo(ctx, v)
}

func NewUnionTestIterator(i1, i2 SetIterator, cntr *int) SetIterator {
	ui := NewUnionIterator(context.Background(), i1, i2).(*UnionIterator)
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

func (i *IntersectionTestIterator) Next(ctx context.Context) Value {
	*i.cntr++
	return i.IntersectionIterator.Next(ctx)
}

func (i *IntersectionTestIterator) SkipTo(ctx context.Context, v Value) Value {
	*i.cntr++
	return i.IntersectionIterator.SkipTo(ctx, v)
}

func NewIntersectionTestIterator(i1, i2 SetIterator, cntr *int) SetIterator {
	ui := NewIntersectionIterator(context.Background(), i1, i2).(*IntersectionIterator)
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
			vals = append(vals, Float(i+(numSets*j)))
		}
		s := NewSet(context.Background(), Format_7_18, vrw, vals...)
		iterSlice = append(iterSlice, s.Iterator(context.Background()))
	}
	return iterSlice
}

func createSetsWithSameNumbers(vrw ValueReadWriter, numSets, numElemsPerSet int) []SetIterator {
	vs := ValueSlice{}
	for j := 0; j < numElemsPerSet; j++ {
		vs = append(vs, Float(j))
	}
	iterSlice := []SetIterator{}
	for i := 0; i < numSets; i++ {
		iterSlice = append(iterSlice, NewSet(context.Background(), Format_7_18, vrw, vs...).Iterator(context.Background()))
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
