package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func TestGetRef(t *testing.T) {
	assert := assert.New(t)
	input := "j false\n"
	h := ref.NewHash()
	h.Write([]byte(input))
	expected := ref.FromHash(h)
	actual := getRef(Bool(false))
	assert.Equal(expected, actual)
}

func TestEnsureRef(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.MemoryStore{}
	count := byte(1)
	mockGetRef := func(v Value) ref.Ref {
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

	getRefOverride = mockGetRef
	defer func() {
		getRefOverride = nil
	}()

	bl := newBlobLeaf([]byte("hi"))
	cb := compoundBlob{[]uint64{2}, []Future{futureFromValue(bl)}, &ref.Ref{}, cs}

	values := []Value{
		newBlobLeaf([]byte{}),
		cb,
		NewList(),
		NewString(""),
		NewMap(),
		NewSet(),
	}
	for i := 0; i < 2; i++ {
		for j, v := range values {
			testRef(v.Ref(), byte(j+1))
		}
	}

	count = byte(1)
	values = []Value{
		Bool(false),
		Int8(0),
		Int16(0),
		Int32(0),
		Int64(0),
		UInt8(0),
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
