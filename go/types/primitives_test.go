// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestPrimitives(t *testing.T) {
	data := []Value{
		Bool(true), Bool(false),
		Number(0), Number(-1),
		Number(-0.1), Number(0.1),
	}

	for i := range data {
		for j := range data {
			if i == j {
				assert.True(t, data[i].Equals(data[j]), "Expected value to equal self at index %d", i)
			} else {
				assert.False(t, data[i].Equals(data[j]), "Expected values at indices %d and %d to not equal", i, j)
			}
		}
	}
}

func TestPrimitivesType(t *testing.T) {
	data := []struct {
		v Value
		k NomsKind
	}{
		{Bool(false), BoolKind},
		{Number(0), NumberKind},
	}

	for _, d := range data {
		assert.True(t, TypeOf(d.v).Equals(MakePrimitiveType(d.k)))
	}
}
