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
	h := vs.WriteValue(s).TargetHash()
	vs.Flush(h)
	v := vs.ReadValue(h) // non-nil
	assert.True(s.Equals(v))
}

func TestValueReadMany(t *testing.T) {
	assert := assert.New(t)

	vals := ValueSlice{String("hello"), Bool(true), Number(42)}
	vs := NewTestValueStore()
	hashes := hash.HashSet{}
	for _, v := range vals {
		h := vs.WriteValue(v).TargetHash()
		hashes.Insert(h)
		vs.Flush(h)
	}

	// Get one Value into vs's Value cache
	vs.ReadValue(vals[0].Hash())

	// Get one Value into vs's pendingPuts
	three := Number(3)
	vals = append(vals, three)
	vs.WriteValue(three)
	hashes.Insert(three.Hash())

	// Add one Value to request that's not in vs
	hashes.Insert(Bool(false).Hash())

	found := map[hash.Hash]Value{}
	foundValues := make(chan Value, len(vals))
	go func() { vs.ReadManyValues(hashes, foundValues); close(foundValues) }()
	for v := range foundValues {
		found[v.Hash()] = v
	}

	assert.Len(found, len(vals))
	for _, v := range vals {
		assert.True(v.Equals(found[v.Hash()]))
	}
}

func TestValueWriteFlush(t *testing.T) {
	assert := assert.New(t)

	vals := ValueSlice{String("hello"), Bool(true), Number(42)}
	vs := NewTestValueStore()
	hashes := hash.HashSet{}
	for _, v := range vals {
		hashes.Insert(vs.WriteValue(v).TargetHash())
	}
	assert.NotZero(vs.pendingPutSize)

	for h := range hashes {
		vs.Flush(h)
	}
	assert.Zero(vs.pendingPutSize)
}

func TestCheckChunksInCache(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	cvs := newLocalValueStore(cs)

	b := NewEmptyBlob()
	cs.Put(EncodeValue(b, nil))
	cvs.set(b.Hash(), hintedChunk{b.Type(), b.Hash()}, false)

	bref := NewRef(b)
	assert.NotPanics(func() { cvs.chunkHintsFromCache(bref) })
}

func TestCheckChunksInCachePostCommit(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	l := NewList()
	r := NewRef(l)
	i := 0
	for r.Height() == 1 {
		l = l.Append(Number(i))
		r = NewRef(l)
		i++
	}

	h := vs.WriteValue(l).TargetHash()
	// Hints for leaf sequences should be absent prior to Flush...
	l.WalkRefs(func(ref Ref) {
		assert.True(vs.check(ref.TargetHash()).Hint().IsEmpty())
	})
	vs.Flush(h)
	// ...And present afterwards
	l.WalkRefs(func(ref Ref) {
		assert.True(vs.check(ref.TargetHash()).Hint() == l.Hash())
	})
}

func TestCheckChunksNotInCacheAfterReadingNovelValue(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	l := NewList()
	r := NewRef(l)
	i := 0
	for r.Height() == 1 {
		l = l.Append(Number(i))
		r = NewRef(l)
		i++
	}

	h := vs.WriteValue(l).TargetHash()
	// Hints for leaf sequences should be absent prior to ReadValue...
	l.WalkRefs(func(ref Ref) {
		assert.True(vs.check(ref.TargetHash()).Hint().IsEmpty())
	})
	vs.ReadValue(h)
	// ...And remain absent!
	l.WalkRefs(func(ref Ref) {
		assert.False(vs.check(ref.TargetHash()).Hint() == l.Hash())
	})
}

func TestCheckChunksInCacheRefValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	cvs := newLocalValueStore(cs)

	l := NewList()
	r := NewRef(l)
	i := 0
	for r.Height() == 1 {
		l = l.Append(Number(i))
		r = NewRef(l)
		i++
	}

	r = cvs.WriteValue(l)
	rr := cvs.WriteValue(ToRefOfValue(r))
	cvs.Flush(rr.TargetHash())

	cvs2 := newLocalValueStore(cs)
	rv := cvs2.ReadValue(rr.TargetHash()).(Ref)
	assert.True(ValueType.Equals(getTargetType(rv)))
	assert.NotPanics(func() { cvs.chunkHintsFromCache(r) })
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

type checkingBatchStore struct {
	BatchStore
	a             *assert.Assertions
	expectedOrder hash.HashSlice
}

func (cbs *checkingBatchStore) expect(rs ...Ref) {
	for _, r := range rs {
		cbs.expectedOrder = append(cbs.expectedOrder, r.TargetHash())
	}
}

func (cbs *checkingBatchStore) SchedulePut(c chunks.Chunk, refHeight uint64, hints Hints) {
	if cbs.a.NotZero(len(cbs.expectedOrder)) {
		cbs.a.Equal(cbs.expectedOrder[0], c.Hash())
		cbs.expectedOrder = cbs.expectedOrder[1:]
	}
}

func (cbs *checkingBatchStore) Flush() {
	cbs.a.Empty(cbs.expectedOrder)
}

func TestFlushOrder(t *testing.T) {
	assert := assert.New(t)
	bs := &checkingBatchStore{NewBatchStoreAdaptor(chunks.NewTestStore()), assert, nil}
	vs := NewValueStore(bs)
	// Graph, which should be flushed grandchildren-first, bottom-up
	//         l
	//        / \
	//      ml1  ml2
	//     /   \    \
	//    b    ml    f
	//        /  \
	//       s    n
	//
	// Expected order: s, n, b, ml, f, ml1, ml2, l
	s := String("oy")
	n := Number(42)
	sr, nr := vs.WriteValue(s), vs.WriteValue(n)
	bs.expect(sr, nr)
	ml := NewList(sr, nr)

	b := NewEmptyBlob()
	br, mlr := vs.WriteValue(b), vs.WriteValue(ml)
	bs.expect(br, mlr)
	ml1 := NewList(br, mlr)

	f := Bool(false)
	fr := vs.WriteValue(f)
	bs.expect(fr)
	ml2 := NewList(fr)

	ml1r, ml2r := vs.WriteValue(ml1), vs.WriteValue(ml2)
	bs.expect(ml1r, ml2r)
	l := NewList(ml1r, ml2r)

	r := vs.WriteValue(l)
	bs.expect(r)
	vs.Flush(l.Hash())
}

func TestFlushOverSize(t *testing.T) {
	assert := assert.New(t)
	bs := &checkingBatchStore{NewBatchStoreAdaptor(chunks.NewTestStore()), assert, nil}
	vs := newValueStoreWithCacheAndPending(bs, 0, 10)

	s := String("oy")
	sr := vs.WriteValue(s)
	l := NewList(sr)
	bs.expect(sr, NewRef(l))

	vs.WriteValue(l)
	vs.Flush(l.Hash())
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
	cvs.Flush(r.TargetHash())

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
	cvs.Flush(r.TargetHash())
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
			_, present := hints[r.TargetHash()]
			assert.True(present)
		}
	}
}

func TestPanicOnReadBadVersion(t *testing.T) {
	cvs := newLocalValueStore(&badVersionStore{chunks.NewTestStore()})
	assert.Panics(t, func() { cvs.ReadValue(hash.Hash{}) })
}

func TestPanicOnWriteBadVersion(t *testing.T) {
	cvs := newLocalValueStore(&badVersionStore{chunks.NewTestStore()})
	assert.Panics(t, func() { r := cvs.WriteValue(NewEmptyBlob()); cvs.Flush(r.TargetHash()) })
}

type badVersionStore struct {
	*chunks.TestStore
}

func (b *badVersionStore) Version() string {
	return "BAD"
}
