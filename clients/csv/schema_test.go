package csv

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestSchemaDetection(t *testing.T) {
	assert := assert.New(t)

	test := func(input [][]string, expect [][]types.NomsKind) {
		options := NewSchemaOptions(len(input[0]))
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
		[][]types.NomsKind{
			[]types.NomsKind{types.StringKind},
			[]types.NomsKind{types.BoolKind, types.StringKind},
			[]types.NomsKind{
				types.Uint8Kind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind,
				types.Int8Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind,
				types.Float32Kind, types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"true"},
			[]string{"1"},
			[]string{"1.1"},
		},
		[][]types.NomsKind{
			[]types.NomsKind{types.StringKind},
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
		[][]types.NomsKind{
			[]types.NomsKind{types.BoolKind, types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"1.1"},
		},
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Float32Kind, types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Float32Kind,
				types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"1"},
			[]string{"0"},
		},
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Uint8Kind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind,
				types.Int8Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind,
				types.Float32Kind,
				types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Int8Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind,
				types.Float32Kind, types.Float64Kind,
				types.StringKind},
		},
	)
	test(
		[][]string{
			[]string{"0"},
			[]string{"-0"},
		},
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Int8Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind,
				types.Float32Kind, types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Int16Kind, types.Int32Kind, types.Int64Kind,
				types.Float32Kind, types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Int16Kind, types.Int32Kind, types.Int64Kind,
				types.Float32Kind, types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Int32Kind, types.Int64Kind,
				types.Float32Kind, types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Int32Kind, types.Int64Kind,
				types.Float32Kind, types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Int64Kind,
				types.Float32Kind, types.Float64Kind,
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
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Int64Kind,
				types.Float32Kind, types.Float64Kind,
				types.StringKind},
		},
	)

	test(
		[][]string{
			[]string{fmt.Sprintf("%d", uint64(1<<63))},
			[]string{fmt.Sprintf("%d", uint64(1<<63)+1)},
		},
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Uint64Kind,
				types.Float32Kind,
				types.Float64Kind,
				types.StringKind},
		},
	)

	test(
		[][]string{
			[]string{fmt.Sprintf("%d", uint64(1<<32))},
			[]string{fmt.Sprintf("%d", uint64(1<<32)+1)},
		},
		[][]types.NomsKind{
			[]types.NomsKind{
				types.Uint64Kind, types.Int64Kind,
				types.Float32Kind,
				types.Float64Kind,
				types.StringKind},
		},
	)
}
