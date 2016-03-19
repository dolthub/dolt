package types

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestGetRef(t *testing.T) {
	assert := assert.New(t)
	input := fmt.Sprintf("t [%d,false]", BoolKind)
	h := ref.NewHash()
	h.Write([]byte(input))
	expected := ref.FromHash(h)
	actual := getRef(Bool(false))
	assert.Equal(expected, actual)
}

func TestEnsureRef(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()
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
	cb := newCompoundBlob([]metaTuple{{bl, Ref{}, Uint64(2)}}, vs)

	ll := newListLeaf(listType, NewString("foo"))
	cl := buildCompoundList([]metaTuple{{ll, Ref{}, Uint64(1)}}, listType, vs)

	ml := newMapLeaf(mapType, mapEntry{NewString("foo"), NewString("bar")})
	cm := buildCompoundMap([]metaTuple{{ml, Ref{}, NewString("foo")}}, mapType, vs)

	sl := newSetLeaf(setType, NewString("foo"))
	cps := buildCompoundSet([]metaTuple{{sl, Ref{}, NewString("foo")}}, setType, vs)

	count = byte(1)
	values := []Value{
		newBlobLeaf([]byte{}),
		cb,
		newListLeaf(listType, NewString("bar")),
		cl,
		NewString(""),
		cm,
		newMapLeaf(mapType),
		cps,
		newSetLeaf(setType),
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
		Uint8(0),
		Uint16(0),
		Uint32(0),
		Uint64(0),
		Float32(0),
		Float64(0),
	}
	for i := 0; i < 2; i++ {
		for j, v := range values {
			testRef(v.Ref(), byte(i*len(values)+(j+1)))
		}
	}
}
