package test

import (
	"testing"

	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestPrimitiveEquals(t *testing.T) {
	assert := assert.New(t)

	values := []func() types.Value{
		func() types.Value { return nil },
		func() types.Value { return types.Bool(false) },
		func() types.Value { return types.Bool(true) },
		func() types.Value { return types.Int16(0) },
		func() types.Value { return types.Int16(1) },
		func() types.Value { return types.Int16(-1) },
		func() types.Value { return types.Int32(0) },
		func() types.Value { return types.Int32(1) },
		func() types.Value { return types.Int32(-1) },
		func() types.Value { return types.Int64(0) },
		func() types.Value { return types.Int64(1) },
		func() types.Value { return types.Int64(-1) },
		func() types.Value { return types.UInt16(0) },
		func() types.Value { return types.UInt16(1) },
		func() types.Value { return types.UInt32(0) },
		func() types.Value { return types.UInt32(1) },
		func() types.Value { return types.UInt64(0) },
		func() types.Value { return types.UInt64(1) },
		func() types.Value { return types.Float32(0) },
		func() types.Value { return types.Float32(-1) },
		func() types.Value { return types.Float32(1) },
		func() types.Value { return types.Float64(0) },
		func() types.Value { return types.Float64(-1) },
		func() types.Value { return types.Float64(1) },
		func() types.Value { return types.NewString("") },
		func() types.Value { return types.NewString("hi") },
		func() types.Value { return types.NewString("bye") },
		func() types.Value { return types.NewBlob([]byte{}) },
		func() types.Value { return types.NewBlob([]byte("hi")) },
		func() types.Value { return types.NewBlob([]byte("bye")) },
		func() types.Value { return types.NewList() },
		func() types.Value { return types.NewList(types.NewString("foo")) },
		func() types.Value { return types.NewList(types.NewString("bar")) },
		func() types.Value { return types.NewMap() },
		func() types.Value { return types.NewMap(types.NewString("a"), types.NewString("a")) },
		func() types.Value { return types.NewSet() },
		func() types.Value { return types.NewSet(types.NewString("hi")) },
	}

	for i, f1 := range values {
		for j, f2 := range values {
			if f1() == nil {
				continue
			}
			if i == j {
				assert.True(f1().Equals(f2()))
			} else {
				assert.False(f1().Equals(f2()))
			}
		}
	}
}
