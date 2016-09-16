// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

func TestEnsureHash(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()
	count := byte(1)
	mockGetRef := func(v Value) hash.Hash {
		d := hash.Digest{}
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
	cb := newBlob(newBlobMetaSequence([]metaTuple{{Ref{}, newOrderedKey(Number(2)), 2, bl}}, vs))

	ll := newList(newListLeafSequence(nil, String("foo")))
	lt := MakeListType(StringType)
	cl := newList(newMetaSequence([]metaTuple{{Ref{}, newOrderedKey(Number(1)), 1, ll}}, lt, vs))

	newStringOrderedKey := func(s string) orderedKey {
		return newOrderedKey(String(s))
	}

	ml := newMap(newMapLeafSequence(nil, mapEntry{String("foo"), String("bar")}))
	cm := newMap(newMetaSequence([]metaTuple{{Ref{}, newStringOrderedKey("foo"), 1, ml}}, MakeMapType(StringType, StringType), vs))

	sl := newSet(newSetLeafSequence(nil, String("foo")))
	cps := newSet(newMetaSequence([]metaTuple{{Ref{}, newStringOrderedKey("foo"), 1, sl}}, MakeSetType(StringType), vs))

	count = byte(1)
	values := []Value{
		newBlob(newBlobLeafSequence(nil, []byte{})),
		cb,
		newList(newListLeafSequence(nil, String("bar"))),
		cl,
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

	for _, v := range values {
		expected := byte(0x42)
		assignHash(v.(hashCacher), hash.New(hash.Digest{0: expected}))
		testRef(v.Hash(), expected)
	}

	count = byte(1)
	values = []Value{
		Bool(false),
		Number(0),
		String(""),
	}
	for i := 0; i < 2; i++ {
		for j, v := range values {
			testRef(v.Hash(), byte(i*len(values)+(j+1)))
		}
	}
}
