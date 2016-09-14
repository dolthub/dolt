// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

type iterator interface {
	Next() Value
}

func iterToSlice(iter iterator) ValueSlice {
	vs := ValueSlice{}
	for {
		v := iter.Next()
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
		vs = append(vs, Number(i))
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
		nums = append(nums, Number(i))
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
		nums = append(nums, NewStruct("num", StructData{"n": Number(i)}))
	}
	return nums
}

func generateNumbersAsRefOfStructs(n int) []Value {
	vs := NewTestValueStore()
	nums := []Value{}
	for i := 0; i < n; i++ {
		r := vs.WriteValue(NewStruct("num", StructData{"n": Number(i)}))
		nums = append(nums, r)
	}
	return nums
}

func chunkDiffCount(c1 []Ref, c2 []Ref) int {
	count := 0
	hashes := make(map[hash.Hash]int)

	for _, r := range c1 {
		hashes[r.TargetHash()]++
	}

	for _, r := range c2 {
		if c, ok := hashes[r.TargetHash()]; ok {
			if c == 1 {
				delete(hashes, r.TargetHash())
			} else {
				hashes[r.TargetHash()] = c - 1
			}
		} else {
			count++
		}
	}

	count += len(hashes)
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
