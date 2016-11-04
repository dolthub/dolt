// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

func TestValueReadWriteRead(t *testing.T) {
	assert := assert.New(t)

	s := String("hello")
	vs := NewTestValueStore()
	assert.Nil(vs.ReadValue(s.Hash())) // nil
	r := vs.WriteValue(s)
	v := vs.ReadValue(r.TargetHash()) // non-nil
	assert.True(s.Equals(v))
}

func TestCheckChunksInCache(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	cvs := newLocalValueStore(cs)

	b := NewEmptyBlob()
	cs.Put(EncodeValue(b, nil))
	cvs.set(b.Hash(), hintedChunk{b.Type(), b.Hash()})

	bref := NewRef(b)
	assert.NotPanics(func() { cvs.chunkHintsFromCache(bref) })
}

func TestCheckChunksNotInCache(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	cvs := newLocalValueStore(cs)

	b := NewEmptyBlob()
	cs.Put(EncodeValue(b, nil))

	bref := NewRef(b)
	assert.Panics(func() { cvs.chunkHintsFromCache(bref) })
}

func TestEnsureChunksInCache(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	cvs := newLocalValueStore(cs)

	b := NewEmptyBlob()
	s := String("oy")
	bref := NewRef(b)
	sref := NewRef(s)
	l := NewList(bref, sref)

	cs.Put(EncodeValue(b, nil))
	cs.Put(EncodeValue(s, nil))
	cs.Put(EncodeValue(l, nil))

	assert.NotPanics(func() { cvs.ensureChunksInCache(bref) })
	assert.NotPanics(func() { cvs.ensureChunksInCache(l) })
}

func TestEnsureChunksFails(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	cvs := newLocalValueStore(cs)

	b := NewEmptyBlob()
	bref := NewRef(b)
	assert.Panics(func() { cvs.ensureChunksInCache(bref) })

	s := String("oy")
	cs.Put(EncodeValue(b, nil))
	cs.Put(EncodeValue(s, nil))

	badRef := constructRef(MakeRefType(MakePrimitiveType(BoolKind)), s.Hash(), 1)
	l := NewList(bref, badRef)

	cs.Put(EncodeValue(l, nil))
	assert.Panics(func() { cvs.ensureChunksInCache(l) })
}

func TestCacheOnReadValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	cvs := newLocalValueStore(cs)

	b := NewEmptyBlob()
	bref := cvs.WriteValue(b)
	r := cvs.WriteValue(bref)

	cvs2 := newLocalValueStore(cs)
	v := cvs2.ReadValue(r.TargetHash())
	assert.True(bref.Equals(v))
	assert.True(cvs2.isPresent(b.Hash()))
	assert.True(cvs2.isPresent(bref.Hash()))
}

func TestHintsOnCache(t *testing.T) {
	assert := assert.New(t)
	cvs := newLocalValueStore(chunks.NewTestStore())

	cr1 := cvs.WriteValue(Number(1))
	cr2 := cvs.WriteValue(Number(2))
	s1 := NewStruct("", StructData{
		"a": cr1,
		"b": cr2,
	})
	r := cvs.WriteValue(s1)
	v := cvs.ReadValue(r.TargetHash())

	if assert.True(v.Equals(s1)) {
		cr3 := cvs.WriteValue(Number(3))
		s2 := NewStruct("", StructData{
			"a": cr1,
			"b": cr2,
			"c": cr3,
		})

		hints := cvs.chunkHintsFromCache(s2)
		if assert.Len(hints, 1) {
			for _, hash := range []hash.Hash{r.TargetHash()} {
				_, present := hints[hash]
				assert.True(present)
			}
		}
	}
}

func TestPanicOnReadBadVersion(t *testing.T) {
	cvs := newLocalValueStore(&badVersionStore{chunks.NewTestStore()})
	assert.Panics(t, func() { cvs.ReadValue(hash.Hash{}) })
}

func TestPanicOnWriteBadVersion(t *testing.T) {
	cvs := newLocalValueStore(&badVersionStore{chunks.NewTestStore()})
	assert.Panics(t, func() { cvs.WriteValue(NewEmptyBlob()) })
}

type badVersionStore struct {
	*chunks.TestStore
}

func (b *badVersionStore) Version() string {
	return "BAD"
}
