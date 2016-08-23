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
			{"foo", "1", "5"},
			{"bar", "0", "10"},
			{"true", "1", "23"},
			{"1", "1", "60"},
			{"1.1", "false", "75"},
		},
		[]KindSlice{
			{types.StringKind},
			{types.BoolKind, types.StringKind},
			{
				types.NumberKind,
				types.StringKind,
			},
		},
	)
	test(
		[][]string{
			{"foo"},
			{"bar"},
			{"true"},
			{"1"},
			{"1.1"},
		},
		[]KindSlice{
			{types.StringKind},
		},
	)
	test(
		[][]string{
			{"true"},
			{"1"},
			{"1.1"},
		},
		[]KindSlice{
			{types.StringKind},
		},
	)
	test(
		[][]string{
			{"true"},
			{"false"},
			{"True"},
			{"False"},
			{"TRUE"},
			{"FALSE"},
			{"1"},
			{"0"},
		},
		[]KindSlice{
			{types.BoolKind, types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"1.1"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"1.1"},
			{"4.940656458412465441765687928682213723651e-50"},
			{"-4.940656458412465441765687928682213723651e-50"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)

	test(
		[][]string{
			{"1"},
			{"1.1"},
			{"1.797693134862315708145274237317043567981e+102"},
			{"-1.797693134862315708145274237317043567981e+102"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"1.1"},
			{"1.797693134862315708145274237317043567981e+309"},
			{"-1.797693134862315708145274237317043567981e+309"},
		},
		[]KindSlice{
			{
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"0"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.BoolKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"0"},
			{"-1"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"0"},
			{"-0"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"280"},
			{"0"},
			{"-1"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"-180"},
			{"0"},
			{"-1"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"33000"},
			{"0"},
			{"-1"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"-44000"},
			{"0"},
			{"-1"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"2547483648"},
			{"0"},
			{"-1"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{"1"},
			{"-4347483648"},
			{"0"},
			{"-1"},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{fmt.Sprintf("%d", uint64(1<<63))},
			{fmt.Sprintf("%d", uint64(1<<63)+1)},
		},
		[]KindSlice{
			{
				types.NumberKind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			{fmt.Sprintf("%d", uint64(1<<32))},
			{fmt.Sprintf("%d", uint64(1<<32)+1)},
		},
		[]KindSlice{
			{
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
		{0},
	})
	test([]int{1}, 1, [][]int{
		{1},
	})
	test([]int{0, 1}, 1, [][]int{
		{0},
		{1},
	})
	test([]int{0, 1}, 2, [][]int{
		{0, 1},
	})
	test([]int{70, 80, 90, 100}, 1, [][]int{
		{70},
		{80},
		{90},
		{100},
	})
	test([]int{70, 80, 90, 100}, 2, [][]int{
		{70, 80},
		{70, 90},
		{70, 100},
		{80, 90},
		{80, 100},
		{90, 100},
	})
	test([]int{70, 80, 90, 100}, 3, [][]int{
		{70, 80, 90},
		{70, 80, 100},
		{70, 90, 100},
		{80, 90, 100},
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
		{0},
	})
	test([]int{1}, 1, 1, [][]int{
		{1},
	})
	test([]int{0, 1}, 1, 2, [][]int{
		{0},
		{1},
		{0, 1},
	})
	test([]int{0, 1}, 2, 2, [][]int{
		{0, 1},
	})
	test([]int{70, 80, 90, 100}, 1, 3, [][]int{
		{70},
		{80},
		{90},
		{100},
		{70, 80},
		{70, 90},
		{70, 100},
		{80, 90},
		{80, 100},
		{90, 100},
		{70, 80, 90},
		{70, 80, 100},
		{70, 90, 100},
		{80, 90, 100},
	})
}
