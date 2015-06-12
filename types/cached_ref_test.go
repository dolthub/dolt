package types

import (
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestCachedRef(t *testing.T) {
	assert := assert.New(t)
	count := byte(1)
	mockReffer := func(v Value) ref.Ref {
		d := ref.Sha1Digest{}
		d[0] = count
		count++
		return ref.New(d)
	}
	testRef := func(r ref.Ref, expected byte) {
		d := r.Digest()
		assert.Equal(expected, d[0])
		for i := 1; i < len(d); i++ {
			assert.Equal(byte(0), d[i])
		}
	}

	Reffer = mockReffer
	values := []Value{
		NewBlob([]byte{}),
		NewList(),
		NewString(""),
		NewMap(),
	}
	for i := 0; i < 2; i++ {
		for j, v := range values {
			testRef(v.Ref(), byte(j+1))
		}
	}

	count = byte(1)
	values = []Value{
		Bool(false),
		Int16(0),
		Int32(0),
		Int64(0),
		UInt16(0),
		UInt32(0),
		UInt64(0),
		Float32(0),
		Float64(0),
	}
	for i := 0; i < 2; i++ {
		for j, v := range values {
			testRef(v.Ref(), byte(i*len(values)+(j+1)))
		}
	}
}
