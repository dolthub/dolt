package types

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/hash"
	"github.com/stretchr/testify/assert"
)

func TestGetRef(t *testing.T) {
	assert := assert.New(t)
	input := fmt.Sprintf("t [%d,false]", BoolKind)
	expected := hash.FromData([]byte(input))
	actual := getHash(Bool(false))
	assert.Equal(expected, actual)
}

func TestEnsureHash(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()
	count := byte(1)
	mockGetRef := func(v Value) hash.Hash {
		d := hash.Sha1Digest{}
		d[0] = count
		count++
		return hash.New(d)
	}
	testRef := func(r hash.Hash, expected byte) {
		d := r.Digest()
		assert.Equal(expected, d[0])
		for i := 1; i < len(d); i++ {
			assert.Equal(byte(0), d[i])
		}
	}

	getHashOverride = mockGetRef
	defer func() {
		getHashOverride = nil
	}()

	bl := newBlob(newBlobLeafSequence(nil, []byte("hi")))
	cb := newBlob(newBlobMetaSequence([]metaTuple{{bl, Ref{}, Number(2), 2}}, vs))

	ll := newList(newListLeafSequence(nil, NewString("foo")))
	lt := MakeListType(StringType)
	cl := newList(newIndexedMetaSequence([]metaTuple{{ll, Ref{}, Number(1), 1}}, lt, vs))

	ml := newMap(newMapLeafSequence(nil, mapEntry{NewString("foo"), NewString("bar")}))
	cm := newMap(newOrderedMetaSequence([]metaTuple{{ml, Ref{}, NewString("foo"), 1}}, MakeMapType(StringType, StringType), vs))

	sl := newSet(newSetLeafSequence(nil, NewString("foo")))
	cps := newSet(newOrderedMetaSequence([]metaTuple{{sl, Ref{}, NewString("foo"), 1}}, MakeSetType(StringType), vs))

	count = byte(1)
	values := []Value{
		newBlob(newBlobLeafSequence(nil, []byte{})),
		cb,
		newList(newListLeafSequence(nil, NewString("bar"))),
		cl,
		NewString(""),
		cm,
		newMap(newMapLeafSequence(nil)),
		cps,
		newSet(newSetLeafSequence(nil)),
	}
	for i := 0; i < 2; i++ {
		for j, v := range values {
			testRef(v.Hash(), byte(j+1))
		}
	}

	count = byte(1)
	values = []Value{
		Bool(false),
		Number(0),
	}
	for i := 0; i < 2; i++ {
		for j, v := range values {
			testRef(v.Hash(), byte(i*len(values)+(j+1)))
		}
	}
}
