// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestSchemaDetection(t *testing.T) {
	assert := assert.New(t)
	test := func(input [][]string, expect []KindSlice) {
		options := newSchemaOptions(len(input[0]))
		for _, values := range input {
			options.Test(values)
		}

		assert.Equal(expect, options.ValidKinds())
	}
	test(
		[][]string{
			[]string{"foo", "1", "5"},
			[]string{"bar", "0", "10"},
			[]string{"true", "1", "23"},
			[]string{"1", "1", "60"},
			[]string{"1.1", "false", "75"},
		},
		[]KindSlice{
			KindSlice{types.StringKind},
			KindSlice{types.BoolKind, types.StringKind},
			KindSlice{
				types.NumberKind,
				types.StringKind,
			},
		},
	)
	test(
		[][]string{
			[]string{"foo"},
			[]string{"bar"},
			[]string{"true"},
			[]string{"1"},
			[]string{"1.1"},
		},
		[]KindSlice{
			KindSlice{types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"true"},
			[]string{"1"},
			[]string{"1.1"},
		},
		[]KindSlice{
			KindSlice{types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"true"},
			[]string{"false"},
			[]string{"True"},
			[]string{"False"},
			[]string{"TRUE"},
			[]string{"FALSE"},
			[]string{"1"},
			[]string{"0"},
		},
		[]KindSlice{
			KindSlice{types.BoolKind, types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"1.1"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"1.1"},
			[]string{"4.940656458412465441765687928682213723651e-50"},
			[]string{"-4.940656458412465441765687928682213723651e-50"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)

	test(
		[][]string{
			[]string{"1"},
			[]string{"1.1"},
			[]string{"1.797693134862315708145274237317043567981e+102"},
			[]string{"-1.797693134862315708145274237317043567981e+102"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"1.1"},
			[]string{"1.797693134862315708145274237317043567981e+309"},
			[]string{"-1.797693134862315708145274237317043567981e+309"},
		},
		[]KindSlice{
			KindSlice{
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"0"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.BoolKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"0"},
			[]string{"-1"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"0"},
			[]string{"-0"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"280"},
			[]string{"0"},
			[]string{"-1"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"-180"},
			[]string{"0"},
			[]string{"-1"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"33000"},
			[]string{"0"},
			[]string{"-1"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"-44000"},
			[]string{"0"},
			[]string{"-1"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"2547483648"},
			[]string{"0"},
			[]string{"-1"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"-4347483648"},
			[]string{"0"},
			[]string{"-1"},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{fmt.Sprintf("%d", uint64(1<<63))},
			[]string{fmt.Sprintf("%d", uint64(1<<63)+1)},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{fmt.Sprintf("%d", uint64(1<<32))},
			[]string{fmt.Sprintf("%d", uint64(1<<32)+1)},
		},
		[]KindSlice{
			KindSlice{
				types.NumberKind,
				types.StringKind},
		},
	)
}

func TestCombinationsWithLength(t *testing.T) {
	assert := assert.New(t)
	test := func(input []int, length int, expect [][]int) {
		combinations := make([][]int, 0)
		combinationsWithLength(input, length, func(combination []int) {
			combinations = append(combinations, append([]int{}, combination...))
		})

		assert.Equal(expect, combinations)
	}
	test([]int{0}, 1, [][]int{
		[]int{0},
	})
	test([]int{1}, 1, [][]int{
		[]int{1},
	})
	test([]int{0, 1}, 1, [][]int{
		[]int{0},
		[]int{1},
	})
	test([]int{0, 1}, 2, [][]int{
		[]int{0, 1},
	})
	test([]int{70, 80, 90, 100}, 1, [][]int{
		[]int{70},
		[]int{80},
		[]int{90},
		[]int{100},
	})
	test([]int{70, 80, 90, 100}, 2, [][]int{
		[]int{70, 80},
		[]int{70, 90},
		[]int{70, 100},
		[]int{80, 90},
		[]int{80, 100},
		[]int{90, 100},
	})
	test([]int{70, 80, 90, 100}, 3, [][]int{
		[]int{70, 80, 90},
		[]int{70, 80, 100},
		[]int{70, 90, 100},
		[]int{80, 90, 100},
	})
}

func TestCombinationsWithLengthFromTo(t *testing.T) {
	assert := assert.New(t)
	test := func(input []int, smallestLength, largestLength int, expect [][]int) {
		combinations := make([][]int, 0)
		combinationsLengthsFromTo(input, smallestLength, largestLength, func(combination []int) {
			combinations = append(combinations, append([]int{}, combination...))
		})

		assert.Equal(expect, combinations)
	}
	test([]int{0}, 1, 1, [][]int{
		[]int{0},
	})
	test([]int{1}, 1, 1, [][]int{
		[]int{1},
	})
	test([]int{0, 1}, 1, 2, [][]int{
		[]int{0},
		[]int{1},
		[]int{0, 1},
	})
	test([]int{0, 1}, 2, 2, [][]int{
		[]int{0, 1},
	})
	test([]int{70, 80, 90, 100}, 1, 3, [][]int{
		[]int{70},
		[]int{80},
		[]int{90},
		[]int{100},
		[]int{70, 80},
		[]int{70, 90},
		[]int{70, 100},
		[]int{80, 90},
		[]int{80, 100},
		[]int{90, 100},
		[]int{70, 80, 90},
		[]int{70, 80, 100},
		[]int{70, 90, 100},
		[]int{80, 90, 100},
	})
}
