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
	if assert.NotNil(v) {
		assert.True(s.Equals(v), "%s != %s", EncodedValue(s), EncodedValue(v))
	}
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

func (cbs *checkingBatchStore) SchedulePut(c chunks.Chunk) {
	if cbs.a.NotZero(len(cbs.expectedOrder)) {
		cbs.a.Equal(cbs.expectedOrder[0], c.Hash())
		cbs.expectedOrder = cbs.expectedOrder[1:]
	}
	cbs.BatchStore.SchedulePut(c)
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
