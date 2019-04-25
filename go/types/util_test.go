// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

type iterator interface {
	Next(ctx context.Context) Value
}

func iterToSlice(iter iterator) ValueSlice {
	vs := ValueSlice{}
	for {
		v := iter.Next(context.Background())
		if v == nil {
			break
		}
		vs = append(vs, v)
	}
	return vs
}

func intsToValueSlice(ints ...int) ValueSlice {
	vs := ValueSlice{}
	for _, i := range ints {
		vs = append(vs, Float(i))
	}
	return vs
}

func generateNumbersAsValues(n int) []Value {
	return generateNumbersAsValuesFromToBy(0, n, 1)
}

func generateNumbersAsValueSlice(n int) ValueSlice {
	return generateNumbersAsValuesFromToBy(0, n, 1)
}

func generateNumbersAsValuesFromToBy(from, to, by int) ValueSlice {
	d.Chk.True(to >= from, "to must be greater than or equal to from")
	d.Chk.True(by > 0, "must be an integer greater than zero")
	nums := []Value{}
	for i := from; i < to; i += by {
		nums = append(nums, Float(i))
	}
	return nums
}

func generateNumbersAsStructs(n int) ValueSlice {
	return generateNumbersAsValuesFromToBy(0, n, 1)
}

func generateNumbersAsStructsFromToBy(from, to, by int) ValueSlice {
	d.Chk.True(to >= from, "to must be greater than or equal to from")
	d.Chk.True(by > 0, "must be an integer greater than zero")
	nums := []Value{}
	for i := from; i < to; i += by {
		nums = append(nums, NewStruct("num", StructData{"n": Float(i)}))
	}
	return nums
}

func generateNumbersAsRefOfStructs(vrw ValueReadWriter, n int) []Value {
	nums := []Value{}
	for i := 0; i < n; i++ {
		r := vrw.WriteValue(context.Background(), NewStruct("num", StructData{"n": Float(i)}))
		nums = append(nums, r)
	}
	return nums
}

func leafCount(c Collection) int {
	leaves, _ := LoadLeafNodes(context.Background(), []Collection{c}, 0, c.Len())
	return len(leaves)
}

func leafDiffCount(c1, c2 Collection) int {
	count := 0
	hashes := make(map[hash.Hash]int)

	leaves1, _ := LoadLeafNodes(context.Background(), []Collection{c1}, 0, c1.Len())
	leaves2, _ := LoadLeafNodes(context.Background(), []Collection{c2}, 0, c2.Len())

	for _, l := range leaves1 {
		hashes[l.Hash()]++
	}

	for _, l := range leaves2 {
		if c, ok := hashes[l.Hash()]; ok {
			if c == 1 {
				delete(hashes, l.Hash())
			} else {
				hashes[l.Hash()] = c - 1
			}
		} else {
			count++
		}
	}

	for _, c := range hashes {
		count += c
	}

	return count
}

func reverseValues(values []Value) []Value {
	newValues := make([]Value, len(values))
	for i := 0; i < len(values); i++ {
		newValues[i] = values[len(values)-i-1]
	}
	return newValues
}

func spliceValues(values []Value, start int, deleteCount int, newItems ...Value) []Value {
	numCurrentItems := len(values)
	numNewItems := len(newItems)
	newArr := make([]Value, numCurrentItems-deleteCount+numNewItems)
	copy(newArr[0:], values[0:start])
	copy(newArr[start:], newItems[0:])
	copy(newArr[start+numNewItems:], values[start+deleteCount:])
	return newArr
}
