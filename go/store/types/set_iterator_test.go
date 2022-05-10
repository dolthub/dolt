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
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numbers := append(generateNumbersAsValues(vs.Format(), 10), Float(20), Float(25))
	s, err := NewSet(context.Background(), vs, numbers...)
	require.NoError(t, err)
	i, err := s.Iterator(context.Background())
	require.NoError(t, err)
	vals, err := iterToSlice(i)
	require.NoError(t, err)
	assert.True(vals.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i, err = s.Iterator(context.Background())
	require.NoError(t, err)
	assert.Panics(func() {
		_, _ = i.SkipTo(context.Background(), nil)
	})
	assert.Equal(Float(0), mustValue(i.SkipTo(context.Background(), Float(-20))))
	assert.Equal(Float(2), mustValue(i.SkipTo(context.Background(), Float(2))))
	assert.Equal(Float(3), mustValue(i.SkipTo(context.Background(), Float(-20))))
	assert.Equal(Float(5), mustValue(i.SkipTo(context.Background(), Float(5))))
	assert.Equal(Float(6), mustValue(i.Next(context.Background())))
	assert.Equal(Float(7), mustValue(i.SkipTo(context.Background(), Float(6))))
	assert.Equal(Float(20), mustValue(i.SkipTo(context.Background(), Float(15))))
	assert.Nil(i.SkipTo(context.Background(), Float(30)))
	assert.Nil(i.SkipTo(context.Background(), Float(30)))
	assert.Nil(i.SkipTo(context.Background(), Float(1)))

	i, err = s.Iterator(context.Background())
	require.NoError(t, err)
	assert.Equal(Float(0), mustValue(i.Next(context.Background())))
	assert.Equal(Float(1), mustValue(i.Next(context.Background())))
	assert.Equal(Float(3), mustValue(i.SkipTo(context.Background(), Float(3))))
	assert.Equal(Float(4), mustValue(i.Next(context.Background())))

	empty, err := NewSet(context.Background(), vs)
	require.NoError(t, err)
	assert.Nil(mustSIter(empty.Iterator(context.Background())).Next(context.Background()))
	assert.Nil(mustSIter(empty.Iterator(context.Background())).SkipTo(context.Background(), Float(-30)))

	set, err := NewSet(context.Background(), vs, Float(42))
	require.NoError(t, err)
	single, err := set.Iterator(context.Background())
	require.NoError(t, err)
	assert.Equal(Float(42), mustValue(single.SkipTo(context.Background(), Float(42))))
	assert.Equal(nil, mustValue(single.SkipTo(context.Background(), Float(42))))

	set, err = NewSet(context.Background(), vs, Float(42))
	require.NoError(t, err)
	single, err = set.Iterator(context.Background())
	require.NoError(t, err)
	assert.Equal(Float(42), mustValue(single.SkipTo(context.Background(), Float(42))))
	assert.Equal(nil, mustValue(single.Next(context.Background())))

	set, err = NewSet(context.Background(), vs, Float(42))
	require.NoError(t, err)
	single, err = set.Iterator(context.Background())
	require.NoError(t, err)
	assert.Equal(Float(42), mustValue(single.SkipTo(context.Background(), Float(21))))
}

func TestSetIteratorAt(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numbers := append(generateNumbersAsValues(vs.Format(), 5), Float(10))
	s, err := NewSet(context.Background(), vs, numbers...)
	require.NoError(t, err)
	i, err := s.IteratorAt(context.Background(), 0)
	require.NoError(t, err)
	vals, err := iterToSlice(i)
	require.NoError(t, err)
	assert.True(vals.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i, err = s.IteratorAt(context.Background(), 2)
	require.NoError(t, err)
	vals, err = iterToSlice(i)
	require.NoError(t, err)
	assert.True(vals.Equals(numbers[2:]), "Expected: %v != actual: %v", numbers[2:], vs)

	i, err = s.IteratorAt(context.Background(), 10)
	require.NoError(t, err)
	vals, err = iterToSlice(i)
	require.NoError(t, err)
	assert.True(vals.Equals(nil), "Expected: %v != actual: %v", nil, vs)
}

func TestSetIteratorFrom(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	numbers := append(generateNumbersAsValues(vs.Format(), 5), Float(10), Float(20))
	s, err := NewSet(context.Background(), vs, numbers...)
	require.NoError(t, err)
	i, err := s.IteratorFrom(context.Background(), Float(0))
	require.NoError(t, err)
	vals, err := iterToSlice(i)
	require.NoError(t, err)
	assert.True(vals.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i, err = s.IteratorFrom(context.Background(), Float(2))
	require.NoError(t, err)
	vals, err = iterToSlice(i)
	require.NoError(t, err)
	assert.True(vals.Equals(numbers[2:]), "Expected: %v != actual: %v", numbers[2:], vs)

	i, err = s.IteratorFrom(context.Background(), Float(10))
	require.NoError(t, err)
	vals, err = iterToSlice(i)
	require.NoError(t, err)
	assert.True(vals.Equals(ValueSlice{Float(10), Float(20)}), "Expected: %v != actual: %v", nil, vs)

	i, err = s.IteratorFrom(context.Background(), Float(20))
	require.NoError(t, err)
	vals, err = iterToSlice(i)
	require.NoError(t, err)
	assert.True(vals.Equals(ValueSlice{Float(20)}), "Expected: %v != actual: %v", nil, vs)

	i, err = s.IteratorFrom(context.Background(), Float(100))
	require.NoError(t, err)
	vals, err = iterToSlice(i)
	require.NoError(t, err)
	assert.True(vals.Equals(nil), "Expected: %v != actual: %v", nil, vs)

	// Not present. Starts at next larger.
	i, err = s.IteratorFrom(context.Background(), Float(15))
	require.NoError(t, err)
	vals, err = iterToSlice(i)
	require.NoError(t, err)
	assert.True(vals.Equals(ValueSlice{Float(20)}), "Expected: %v != actual: %v", nil, vs)
}

func TestUnionIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	set1, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 0, 10, 1)...)
	set2, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 5, 15, 1)...)
	set3, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 10, 20, 1)...)
	set4, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 15, 25, 1)...)

	ui1, err := NewUnionIterator(context.Background(), vs.Format(), mustSIter(set1.Iterator(context.Background())), mustSIter(set2.Iterator(context.Background())))
	vals, err := iterToSlice(ui1)
	expectedRes := generateNumbersAsValues(vs.Format(), 15)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	ui1, err = NewUnionIterator(context.Background(), vs.Format(), mustSIter(set1.Iterator(context.Background())), mustSIter(set4.Iterator(context.Background())))
	ui2, err := NewUnionIterator(context.Background(), vs.Format(), mustSIter(set3.Iterator(context.Background())), mustSIter(set2.Iterator(context.Background())))
	ui3, err := NewUnionIterator(context.Background(), vs.Format(), ui1, ui2)
	vals, err = iterToSlice(ui3)
	expectedRes = generateNumbersAsValues(vs.Format(), 25)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	ui1, err = NewUnionIterator(context.Background(), vs.Format(), mustSIter(set1.Iterator(context.Background())), mustSIter(set4.Iterator(context.Background())))
	ui2, err = NewUnionIterator(context.Background(), vs.Format(), mustSIter(set3.Iterator(context.Background())), mustSIter(set2.Iterator(context.Background())))
	ui3, err = NewUnionIterator(context.Background(), vs.Format(), ui1, ui2)

	assert.Panics(func() {
		_, _ = ui3.SkipTo(context.Background(), nil)
		assert.Error(err)
	})
	assert.Equal(Float(0), mustValue(ui3.SkipTo(context.Background(), Float(-5))))
	assert.Equal(Float(5), mustValue(ui3.SkipTo(context.Background(), Float(5))))
	assert.Equal(Float(8), mustValue(ui3.SkipTo(context.Background(), Float(8))))
	assert.Equal(Float(9), mustValue(ui3.SkipTo(context.Background(), Float(8))))
	assert.Equal(Float(10), mustValue(ui3.SkipTo(context.Background(), Float(8))))
	assert.Equal(Float(11), mustValue(ui3.SkipTo(context.Background(), Float(7))))
	assert.Equal(Float(12), mustValue(ui3.Next(context.Background())))
	assert.Equal(Float(15), mustValue(ui3.SkipTo(context.Background(), Float(15))))
	assert.Equal(Float(24), mustValue(ui3.SkipTo(context.Background(), Float(24))))
	assert.Nil(ui3.SkipTo(context.Background(), Float(25)))

	singleElemSet, err := NewSet(context.Background(), vs, Float(4))
	emptySet, err := NewSet(context.Background(), vs)

	ui10, err := NewUnionIterator(context.Background(), vs.Format(), mustSIter(singleElemSet.Iterator(context.Background())), mustSIter(singleElemSet.Iterator(context.Background())))
	ui20, err := NewUnionIterator(context.Background(), vs.Format(), mustSIter(emptySet.Iterator(context.Background())), mustSIter(emptySet.Iterator(context.Background())))
	ui30, err := NewUnionIterator(context.Background(), vs.Format(), ui10, ui20)
	vals, err = iterToSlice(ui30)
	expectedRes = ValueSlice{Float(4)}
	assert.True(vals.Equals(expectedRes), "%v != %v\n", expectedRes, vs)
}

func TestIntersectionIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	byTwos, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 0, 200, 2)...)
	require.NoError(t, err)
	byThrees, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 0, 200, 3)...)
	require.NoError(t, err)
	byFives, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 0, 200, 5)...)
	require.NoError(t, err)

	i1, err := NewIntersectionIterator(context.Background(), vs.Format(), mustSIter(byTwos.Iterator(context.Background())), mustSIter(byThrees.Iterator(context.Background())))
	require.NoError(t, err)
	vals, err := iterToSlice(i1)
	require.NoError(t, err)
	expectedRes := generateNumbersAsValuesFromToBy(vs.Format(), 0, 200, 6)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	it1, err := NewIntersectionIterator(context.Background(), vs.Format(), mustSIter(byTwos.Iterator(context.Background())), mustSIter(byThrees.Iterator(context.Background())))
	require.NoError(t, err)
	it2, err := NewIntersectionIterator(context.Background(), vs.Format(), it1, mustSIter(byFives.Iterator(context.Background())))
	require.NoError(t, err)
	vals, err = iterToSlice(it2)
	require.NoError(t, err)
	expectedRes = generateNumbersAsValuesFromToBy(vs.Format(), 0, 200, 30)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	it1, err = NewIntersectionIterator(context.Background(), vs.Format(), mustSIter(byThrees.Iterator(context.Background())), mustSIter(byFives.Iterator(context.Background())))
	require.NoError(t, err)
	it2, err = NewIntersectionIterator(context.Background(), vs.Format(), it1, mustSIter(byTwos.Iterator(context.Background())))
	require.NoError(t, err)

	assert.Panics(func() {
		_, _ = it2.SkipTo(context.Background(), nil)
	})
	assert.Equal(Float(30), mustValue(it2.SkipTo(context.Background(), Float(5))))
	assert.Equal(Float(60), mustValue(it2.SkipTo(context.Background(), Float(60))))
	assert.Equal(Float(90), mustValue(it2.SkipTo(context.Background(), Float(5))))
	assert.Equal(Float(120), mustValue(it2.Next(context.Background())))
	assert.Equal(Float(150), mustValue(it2.SkipTo(context.Background(), Float(150))))
	assert.Nil(it2.SkipTo(context.Background(), Float(40000)))
}

func TestCombinationIterator(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	byTwos, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 0, 70, 2)...)
	require.NoError(t, err)
	byThrees, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 0, 70, 3)...)
	require.NoError(t, err)
	byFives, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 0, 70, 5)...)
	require.NoError(t, err)
	bySevens, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 0, 70, 7)...)
	require.NoError(t, err)

	it1, err := NewIntersectionIterator(context.Background(), vs.Format(), mustSIter(byTwos.Iterator(context.Background())), mustSIter(bySevens.Iterator(context.Background())))
	require.NoError(t, err)
	it2, err := NewIntersectionIterator(context.Background(), vs.Format(), mustSIter(byFives.Iterator(context.Background())), mustSIter(byThrees.Iterator(context.Background())))
	require.NoError(t, err)
	ut1, err := NewUnionIterator(context.Background(), vs.Format(), it1, it2)
	require.NoError(t, err)
	vals, err := iterToSlice(ut1)
	require.NoError(t, err)
	expectedRes := intsToValueSlice(0, 14, 15, 28, 30, 42, 45, 56, 60)
	require.NoError(t, err)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)

	ut1, err = NewUnionIterator(context.Background(), vs.Format(), mustSIter(byTwos.Iterator(context.Background())), mustSIter(bySevens.Iterator(context.Background())))
	require.NoError(t, err)
	it2, err = NewIntersectionIterator(context.Background(), vs.Format(), mustSIter(byFives.Iterator(context.Background())), mustSIter(byThrees.Iterator(context.Background())))
	require.NoError(t, err)
	ut2, err := NewIntersectionIterator(context.Background(), vs.Format(), ut1, it2)
	require.NoError(t, err)
	vals, err = iterToSlice(ut2)
	require.NoError(t, err)
	expectedRes = intsToValueSlice(0, 30, 60)
	assert.True(vals.Equals(expectedRes), "Expected: %v != actual: %v", expectedRes, vs)
}

type UnionTestIterator struct {
	*UnionIterator
	cntr *int
}

func (ui *UnionTestIterator) Next(ctx context.Context) (Value, error) {
	*ui.cntr++
	return ui.UnionIterator.Next(ctx)
}

func (ui *UnionTestIterator) SkipTo(ctx context.Context, v Value) (Value, error) {
	*ui.cntr++
	return ui.UnionIterator.SkipTo(ctx, v)
}

func NewUnionTestIterator(nbf *NomsBinFormat) func(i1, i2 SetIterator, cntr *int) (SetIterator, error) {
	return func(i1, i2 SetIterator, cntr *int) (SetIterator, error) {
		ui, err := NewUnionIterator(context.Background(), nbf, i1, i2)

		if err != nil {
			return nil, err
		}

		return &UnionTestIterator{ui.(*UnionIterator), cntr}, nil
	}
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
	itrs, err := createSetsWithDistinctNumbers(vs, numSets, numElemsPerSet)
	require.NoError(t, err)
	iter, err := iterize(itrs, NewUnionTestIterator(vs.Format()), &callCount1)
	require.NoError(t, err)
	vals, err := iterToSlice(iter)
	require.NoError(t, err)
	expected := generateNumbersAsValueSlice(vs.Format(), numSets*numElemsPerSet)
	assert.True(expected.Equals(vals), "expected: %v != actual: %v", expected, vals)
	assert.True(expectedMax > callCount1, "callCount: %d exceeds expectedMax: %d", callCount1, expectedMax)

	callCount2 := 0
	itrs, err = createSetsWithSameNumbers(vs, numSets, numElemsPerSet)
	require.NoError(t, err)
	iter, err = iterize(itrs, NewUnionTestIterator(vs.Format()), &callCount2)
	require.NoError(t, err)
	vals, err = iterToSlice(iter)
	require.NoError(t, err)
	expected = generateNumbersAsValueSlice(vs.Format(), numElemsPerSet)
	assert.True(expected.Equals(vals), "expected: %v != actual: %v", expected, vals)
	assert.True(expectedMax > callCount2, "callCount: %d exceeds expectedMax: %d", callCount2, expectedMax)
}

type IntersectionTestIterator struct {
	*IntersectionIterator
	cntr *int
}

func (i *IntersectionTestIterator) Next(ctx context.Context) (Value, error) {
	*i.cntr++
	return i.IntersectionIterator.Next(ctx)
}

func (i *IntersectionTestIterator) SkipTo(ctx context.Context, v Value) (Value, error) {
	*i.cntr++
	return i.IntersectionIterator.SkipTo(ctx, v)
}

func NewIntersectionTestIterator(nbf *NomsBinFormat) func(i1, i2 SetIterator, cntr *int) (SetIterator, error) {
	return func(i1, i2 SetIterator, cntr *int) (SetIterator, error) {
		ui, err := NewIntersectionIterator(context.Background(), nbf, i1, i2)

		if err != nil {
			return nil, err
		}

		return &IntersectionTestIterator{ui.(*IntersectionIterator), cntr}, nil
	}
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
	itrs, err := createSetsWithDistinctNumbers(vs, numSets, numElemsPerSet)
	require.NoError(t, err)
	iter, err := iterize(itrs, NewIntersectionTestIterator(vs.Format()), &callCount1)
	require.NoError(t, err)
	vals, err := iterToSlice(iter)
	require.NoError(t, err)
	expected := ValueSlice{}
	assert.True(expected.Equals(vals), "expected: %v != actual: %v", expected, vals)
	assert.True(expectedMax > callCount1, "callCount: %d exceeds expectedMax: %d", callCount1, expectedMax)

	callCount2 := 0
	itrs, err = createSetsWithSameNumbers(vs, numSets, numElemsPerSet)
	require.NoError(t, err)
	iter, err = iterize(itrs, NewIntersectionTestIterator(vs.Format()), &callCount2)
	require.NoError(t, err)
	vals, err = iterToSlice(iter)
	require.NoError(t, err)
	expected = generateNumbersAsValueSlice(vs.Format(), numElemsPerSet)
	assert.True(expected.Equals(vals), "expected: %v != actual: %v", expected, vals)
	assert.True(expectedMax > callCount2, "callCount: %d exceeds expectedMax: %d", callCount2, expectedMax)
}

func createSetsWithDistinctNumbers(vrw ValueReadWriter, numSets, numElemsPerSet int) ([]SetIterator, error) {
	iterSlice := []SetIterator{}

	for i := 0; i < numSets; i++ {
		vals := ValueSlice{}
		for j := 0; j < numElemsPerSet; j++ {
			vals = append(vals, Float(i+(numSets*j)))
		}
		s, err := NewSet(context.Background(), vrw, vals...)

		if err != nil {
			return nil, err
		}

		itr, err := s.Iterator(context.Background())

		if err != nil {
			return nil, err
		}

		iterSlice = append(iterSlice, itr)
	}
	return iterSlice, nil
}

func createSetsWithSameNumbers(vrw ValueReadWriter, numSets, numElemsPerSet int) ([]SetIterator, error) {
	vs := ValueSlice{}
	for j := 0; j < numElemsPerSet; j++ {
		vs = append(vs, Float(j))
	}
	iterSlice := []SetIterator{}
	for i := 0; i < numSets; i++ {
		s, err := NewSet(context.Background(), vrw, vs...)

		if err != nil {
			return nil, err
		}

		itr, err := s.Iterator(context.Background())

		if err != nil {
			return nil, err
		}

		iterSlice = append(iterSlice, itr)
	}

	return iterSlice, nil
}

type newIterFunc func(i1, i2 SetIterator, cntr *int) (SetIterator, error)

// Iterize calls itself recursively to build a binary tree of iterators over the original set.
func iterize(iters []SetIterator, newIter newIterFunc, cntr *int) (SetIterator, error) {
	if len(iters) == 0 {
		return nil, nil
	}
	if len(iters) <= 1 {
		return iters[0], nil
	}
	var iter0 SetIterator
	newIters := []SetIterator{}
	for i, iter := range iters {
		if i%2 == 0 {
			iter0 = iter
		} else {
			ni, err := newIter(iter0, iter, cntr)

			if err != nil {
				return nil, err
			}

			newIters = append(newIters, ni)
			iter0 = nil
		}
	}
	if iter0 != nil {
		newIters = append(newIters, iter0)
	}
	return iterize(newIters, newIter, cntr)
}
