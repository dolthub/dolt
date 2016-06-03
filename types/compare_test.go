// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestTotalOrdering(t *testing.T) {
	assert := assert.New(t)

	// values in increasing order. Some of these are compared by ref so changing the serialization might change the ordering.
	values := []Value{
		Bool(false), Bool(true),
		Number(-10), Number(0), Number(10),
		NewString("a"), NewString("b"), NewString("c"),

		// The order of these are done by the hash.
		BoolType,
		NewSet(Number(0), Number(1), Number(2), Number(3)),

		// Value - values cannot be value
		// Cycle - values cannot be cycle
		// Union - values cannot be unions
	}

	for i, vi := range values {
		for j, vj := range values {
			if i == j {
				assert.True(vi.Equals(vj))
			} else if i < j {
				x := vi.Less(vj)
				assert.True(x)
			} else {
				x := vi.Less(vj)
				assert.False(x)
			}
		}
	}
}
