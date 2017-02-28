// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package ngql

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestGetArgValue(tt *testing.T) {
	assert := assert.New(tt)

	t := func(expected types.Value, arg interface{}) {
		act, err := getArgValue(arg, expected.Type())
		assert.NoError(err)
		assert.True(act.Equals(expected))
	}

	t(types.Bool(true), true)
	t(types.Number(42), float64(42))
	t(types.String("hi"), "hi")

	_, err := getArgValue(float64(42), types.BoolType)
	assert.Equal("Number is not a subtype of Bool", err.Error())

	n, err := getArgValue(float64(42), types.MakeUnionType(types.BoolType, types.NumberType))
	assert.NoError(err)
	assert.Equal(n, types.Number(42))

	b, err := getArgValue(false, types.MakeUnionType(types.BoolType, types.NumberType))
	assert.NoError(err)
	assert.Equal(b, types.Bool(false))

	type S struct {
		X float64
	}
	_, err = getArgValue(S{42}, types.ValueType)
	assert.Equal("Unsupported type in GraphQL argument, ngql.S, Value", err.Error())

}
