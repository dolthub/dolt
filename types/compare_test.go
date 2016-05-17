package types

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTotalOrdering(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	// values in increasing order. Some of these are compared by ref so changing the serialization might change the ordering.
	values := []Value{
		Bool(false), Bool(true),
		Number(-10), Number(0), Number(10),
		NewString("a"), NewString("b"), NewString("c"),

		// The order of these are done by the hash.
		vs.WriteValue(Number(10)),
		NewSet(Number(0), Number(1), Number(2), Number(3)),
		NewMap(Number(0), Number(1), Number(2), Number(3)),
		BoolType,
		NewBlob(bytes.NewBuffer([]byte{0x00, 0x01, 0x02, 0x03})),
		NewList(Number(0), Number(1), Number(2), Number(3)),
		NewStruct("a", structData{"x": Number(1), "s": NewString("a")}),

		// Value - values cannot be value
		// Parent - values cannot be parent
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
