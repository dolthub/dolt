// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrimitives(t *testing.T) {
	data := []Value{
		Bool(true), Bool(false),
		Float(0), Float(-1),
		Float(-0.1), Float(0.1),
	}

	for i := range data {
		for j := range data {
			if i == j {
				assert.True(t, data[i].Equals(Format_7_18, data[j]), "Expected value to equal self at index %d", i)
			} else {
				assert.False(t, data[i].Equals(Format_7_18, data[j]), "Expected values at indices %d and %d to not equal", i, j)
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
		{Float(0), FloatKind},
	}

	for _, d := range data {
		assert.True(t, TypeOf(d.v).Equals(Format_7_18, MakePrimitiveType(d.k)))
	}
}
