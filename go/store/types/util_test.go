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

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

type iterator interface {
	Next(ctx context.Context) (Value, error)
}

func iterToSlice(iter iterator) (ValueSlice, error) {
	vs := ValueSlice{}
	for {
		v, err := iter.Next(context.Background())

		if err != nil {
			return nil, err
		}

		if v == nil {
			break
		}
		vs = append(vs, v)
	}
	return vs, nil
}

func intsToValueSlice(ints ...int) ValueSlice {
	vs := ValueSlice{}
	for _, i := range ints {
		vs = append(vs, Float(i))
	}
	return vs
}

func generateNumbersAsValues(nbf *NomsBinFormat, n int) []Value {
	return generateNumbersAsValuesFromToBy(nbf, 0, n, 1)
}

func generateNumbersAsValueSlice(nbf *NomsBinFormat, n int) ValueSlice {
	return generateNumbersAsValuesFromToBy(nbf, 0, n, 1)
}

func generateNumbersAsValuesFromToBy(nbf *NomsBinFormat, from, to, by int) ValueSlice {
	d.Chk.True(to >= from, "to must be greater than or equal to from")
	d.Chk.True(by > 0, "must be an integer greater than zero")
	nums := []Value{}
	for i := from; i < to; i += by {
		nums = append(nums, Float(i))
	}
	return nums
}

func generateNumbersAsStructsFromToBy(nbf *NomsBinFormat, from, to, by int) ValueSlice {
	d.Chk.True(to >= from, "to must be greater than or equal to from")
	d.Chk.True(by > 0, "must be an integer greater than zero")
	nums := []Value{}
	for i := from; i < to; i += by {
		nums = append(nums, mustValue(NewStruct(nbf, "num", StructData{"n": Float(i)})))
	}
	return nums
}

func generateNumbersAsRefOfStructs(vrw ValueReadWriter, n int) []Value {
	nums := []Value{}
	for i := 0; i < n; i++ {
		r, err := vrw.WriteValue(context.Background(), mustValue(NewStruct(vrw.Format(), "num", StructData{"n": Float(i)})))
		d.PanicIfError(err)
		nums = append(nums, r)
	}
	return nums
}

func leafCount(c Collection) int {
	leaves, _, err := LoadLeafNodes(context.Background(), []Collection{c}, 0, c.Len())
	d.PanicIfError(err)
	return len(leaves)
}

func leafDiffCount(c1, c2 Collection) int {
	count := 0
	hashes := make(map[hash.Hash]int)

	leaves1, _, err := LoadLeafNodes(context.Background(), []Collection{c1}, 0, c1.Len())
	d.PanicIfError(err)
	leaves2, _, err := LoadLeafNodes(context.Background(), []Collection{c2}, 0, c2.Len())
	d.PanicIfError(err)

	nbf := c1.asSequence().format()

	for _, l := range leaves1 {
		h, err := l.Hash(nbf)
		d.PanicIfError(err)
		hashes[h]++
	}

	for _, l := range leaves2 {
		if c, ok := hashes[mustHash(l.Hash(nbf))]; ok {
			if c == 1 {
				delete(hashes, mustHash(l.Hash(nbf)))
			} else {
				hashes[mustHash(l.Hash(nbf))] = c - 1
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
