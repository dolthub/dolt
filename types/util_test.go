package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

var generateNumbersAsValues = func(n int) []Value {
	d.Chk.True(n > 0, "must be an integer greater than zero")
	return generateNumbersAsValuesFromToBy(0, n, 1)
}

var generateNumbersAsValuesFromToBy = func(from int, to int, by int) []Value {
	d.Chk.True(to > from, "to must be greater than from")
	d.Chk.True(by > 0, "must be an integer greater than zero")
	nums := []Value{}
	for i := from; i < to; i += by {
		nums = append(nums, Number(i))
	}
	return nums
}

var generateNumbersAsStructs = func(n int) []Value {
	d.Chk.True(n > 0, "must be an integer greater than zero")
	nums := []Value{}
	for i := 0; i < n; i++ {
		nums = append(nums, NewStruct("num", structData{"n": Number(i)}))
	}
	return nums
}

var generateNumbersAsRefOfStructs = func(n int) []Value {
	d.Chk.True(n > 0, "must be an integer greater than zero")
	vs := NewTestValueStore()
	nums := []Value{}
	for i := 0; i < n; i++ {
		r := vs.WriteValue(NewStruct("num", structData{"n": Number(i)}))
		nums = append(nums, r)
	}
	return nums
}

func chunkDiffCount(c1 []Ref, c2 []Ref) int {
	count := 0
	refs := make(map[ref.Ref]int)

	for _, r := range c1 {
		refs[r.TargetRef()]++
	}

	for _, r := range c2 {
		if c, ok := refs[r.TargetRef()]; ok {
			if c == 1 {
				delete(refs, r.TargetRef())
			} else {
				refs[r.TargetRef()] = c - 1
			}
		} else {
			count++
		}
	}

	count += len(refs)
	return count
}
