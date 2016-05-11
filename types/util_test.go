package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

var generateNumbersAsValues = func(n int) []Value {
	d.Chk.True(n > 0, "must be an integer greater than zero")
	nums := []Value{}
	for i := 0; i < n; i++ {
		nums = append(nums, Number(i))
	}
	return nums
}

var generateNumbersAsStructs = func(n int) (*Type, []Value) {
	d.Chk.True(n > 0, "must be an integer greater than zero")
	structType := MakeStructType("num", TypeMap{"n": NumberType})
	nums := []Value{}
	for i := 0; i < n; i++ {
		nums = append(nums, NewStruct(structType, structData{"n": Number(i)}))
	}
	return structType, nums
}

var generateNumbersAsRefOfStructs = func(n int) (*Type, []Value) {
	d.Chk.True(n > 0, "must be an integer greater than zero")
	structType := MakeStructType("num", TypeMap{"n": NumberType})
	vs := NewTestValueStore()
	nums := []Value{}
	for i := 0; i < n; i++ {
		r := vs.WriteValue(NewStruct(structType, structData{"n": Number(i)}))
		nums = append(nums, r)
	}
	return structType, nums
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
