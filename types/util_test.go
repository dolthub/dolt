package types

import (
	"github.com/attic-labs/noms/d"
)

var generateNumbersAsValues = func(n int) []Value {
	d.Chk.True(n >= 0, "")
	nums := []Value{}
	for i := 0; i < n; i++ {
		nums = append(nums, Number(i))
	}
	return nums
}

var generateNumbersAsRefOfStructs = func(n int) (*Type, []Value) {
	d.Chk.True(n >= 0, "")
	structType := MakeStructType("num", TypeMap{"n": NumberType})
	vs := NewTestValueStore()
	nums := []Value{}
	for i := 0; i < n; i++ {
		r := vs.WriteValue(NewStruct(structType, structData{"n": Number(i)}))
		nums = append(nums, r)
	}
	return structType, nums
}

var generateNumbersAsStructs = func(n int) (*Type, []Value) {
	d.Chk.True(n >= 0, "")
	structType := MakeStructType("num", TypeMap{"n": NumberType})
	nums := []Value{}
	for i := 0; i < n; i++ {
		nums = append(nums, NewStruct(structType, structData{"n": Number(i)}))
	}
	return structType, nums
}
