package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrimitives(t *testing.T) {
	data := []Value{
		Bool(true), Bool(false),
		Int16(0), Int16(-1),
		Int32(0), Int32(-1),
		Int64(0), Int64(-1),
		UInt16(0), UInt16(1),
		UInt32(0), UInt32(1),
		UInt64(0), UInt64(1),
		Float32(0.0), Float32(0.1),
		Float64(0.0), Float64(0.1),
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
