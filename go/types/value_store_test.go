// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/stretchr/testify/assert"
)

func TestValueReadWriteRead(t *testing.T) {
	assert := assert.New(t)

	s := String("hello")
	vs := newTestValueStore()
	assert.Nil(vs.ReadValue(s.Hash())) // nil
	h := vs.WriteValue(s).TargetHash()
	vs.Commit(vs.Root(), vs.Root())
	v := vs.ReadValue(h) // non-nil
	if assert.NotNil(v) {
		assert.True(s.Equals(v), "%s != %s", EncodedValue(s), EncodedValue(v))
	}
}

func TestReadWriteCache(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.TestStorage{}
	ts := storage.NewView()
	vs := NewValueStore(ts)

	var v Value = Bool(true)
	r := vs.WriteValue(v)
	assert.NotEqual(hash.Hash{}, r.TargetHash())
	vs.Commit(vs.Root(), vs.Root())
	assert.Equal(1, ts.Writes)

	v = vs.ReadValue(r.TargetHash())
	assert.True(v.Equals(Bool(true)))
	assert.Equal(1, ts.Reads)

	v = vs.ReadValue(r.TargetHash())
	assert.True(v.Equals(Bool(true)))
	assert.Equal(1, ts.Reads)
}

func TestValueReadMany(t *testing.T) {
	assert := assert.New(t)

	vals := ValueSlice{String("hello"), Bool(true), Number(42)}
	vs := newTestValueStore()
	hashes := hash.HashSlice{}
	for _, v := range vals {
		h := vs.WriteValue(v).TargetHash()
		hashes = append(hashes, h)
		vs.Commit(vs.Root(), vs.Root())
	}

	// Get one Value into vs's Value cache
	vs.ReadValue(vals[0].Hash())

	// Get one Value into vs's pendingPuts
	three := Number(3)
	vals = append(vals, three)
	vs.WriteValue(three)
	hashes = append(hashes, three.Hash())

	// Add one Value to request that's not in vs
	hashes = append(hashes, Bool(false).Hash())

	found := map[hash.Hash]Value{}
	readValues := vs.ReadManyValues(hashes)

	for i, v := range readValues {
		if v != nil {
			found[hashes[i]] = v
		}
	}

	assert.Len(found, len(vals))
	for _, v := range vals {
		assert.True(v.Equals(found[v.Hash()]))
	}
}

func TestValueWriteFlush(t *testing.T) {
	assert := assert.New(t)

	vals := ValueSlice{String("hello"), Bool(true), Number(42)}
	vs := newTestValueStore()
	hashes := hash.HashSet{}
	for _, v := range vals {
		hashes.Insert(vs.WriteValue(v).TargetHash())
	}
	assert.NotZero(vs.bufferedChunkSize)

	vs.Commit(vs.Root(), vs.Root())
	assert.Zero(vs.bufferedChunkSize)
}

type checkingChunkStore struct {
	chunks.ChunkStore
	a             *assert.Assertions
	expectedOrder hash.HashSlice
}

func (cbs *checkingChunkStore) expect(rs ...Ref) {
	for _, r := range rs {
		cbs.expectedOrder = append(cbs.expectedOrder, r.TargetHash())
	}
}

func (cbs *checkingChunkStore) Put(c chunks.Chunk) {
	if cbs.a.NotZero(len(cbs.expectedOrder), "Unexpected Put of %s", c.Hash()) {
		cbs.a.Equal(cbs.expectedOrder[0], c.Hash())
		cbs.expectedOrder = cbs.expectedOrder[1:]
	}
	cbs.ChunkStore.Put(c)
}

func (cbs *checkingChunkStore) Flush() {
	cbs.a.Empty(cbs.expectedOrder)
}

func TestFlushOrder(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.TestStorage{}
	ccs := &checkingChunkStore{storage.NewView(), assert, nil}
	vs := NewValueStore(ccs)
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
	ccs.expect(sr, nr)
	ml := NewList(vs, sr, nr)

	b := NewEmptyBlob(vs)
	br, mlr := vs.WriteValue(b), vs.WriteValue(ml)
	ccs.expect(br, mlr)
	ml1 := NewList(vs, br, mlr)

	f := Bool(false)
	fr := vs.WriteValue(f)
	ccs.expect(fr)
	ml2 := NewList(vs, fr)

	ml1r, ml2r := vs.WriteValue(ml1), vs.WriteValue(ml2)
	ccs.expect(ml1r, ml2r)
	l := NewList(vs, ml1r, ml2r)

	r := vs.WriteValue(l)
	ccs.expect(r)
	vs.Commit(vs.Root(), vs.Root())
}

func TestFlushOverSize(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.TestStorage{}
	ccs := &checkingChunkStore{storage.NewView(), assert, nil}
	vs := newValueStoreWithCacheAndPending(ccs, 0, 30)

	s := String("oy")
	sr := vs.WriteValue(s)
	ccs.expect(sr)
	NewList(vs, sr) // will write the root chunk
}

func TestTolerateTopDown(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.TestStorage{}
	ccs := &checkingChunkStore{storage.NewView(), assert, nil}
	vs := NewValueStore(ccs)
	// Once the L-ML-S portion of this graph is written once, it's legal to make a Struct ST that contains a ref directly to ML and write it. Then you can write S and ML and Flush ST, which contitutes top-down writing.
	//       L  ST
	//        \ /
	//        ML
	//        /
	//       S
	S := String("oy")
	sr := vs.WriteValue(S)
	ccs.expect(sr)

	ML := NewList(vs, sr)
	mlr := vs.WriteValue(ML)
	ccs.expect(mlr)

	L := NewList(vs, mlr)
	lr := vs.WriteValue(L)
	ccs.expect(lr)

	vs.Commit(vs.Root(), vs.Root())

	assert.Zero(len(vs.bufferedChunks))

	ST := NewStruct("", StructData{"r": mlr})
	str := vs.WriteValue(ST) // ST into bufferedChunks
	vs.WriteValue(S)         // S into bufferedChunks
	vs.WriteValue(ML)        // ML into bufferedChunks AND withBufferedChunks

	// At this point, ValueStore believes ST is a standalone chunk, and that ML -> S
	// So, it'll look at ML, the one parent it knows about, first and write its child (S). Then, it'll write ML, and then it'll flush the remaining buffered chunks, which is just ST.
	ccs.expect(sr, mlr, str)
	vs.Commit(vs.Root(), vs.Root())
}

func TestPanicOnBadVersion(t *testing.T) {
	storage := &chunks.MemoryStorage{}
	t.Run("Read", func(t *testing.T) {
		cvs := NewValueStore(&badVersionStore{ChunkStore: storage.NewView()})
		assert.Panics(t, func() { cvs.ReadValue(hash.Hash{}) })
	})
	t.Run("Write", func(t *testing.T) {
		cvs := NewValueStore(&badVersionStore{ChunkStore: storage.NewView()})
		assert.Panics(t, func() {
			cvs.WriteValue(NewEmptyBlob(cvs))
			cvs.Commit(cvs.Root(), cvs.Root())
		})
	})
}

func TestPanicIfDangling(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	r := NewRef(Bool(true))
	l := NewList(vs, r)
	vs.WriteValue(l)

	assert.Panics(func() {
		vs.Commit(vs.Root(), vs.Root())
	})
}

func TestSkipEnforceCompleteness(t *testing.T) {
	vs := newTestValueStore()
	vs.SetEnforceCompleteness(false)

	r := NewRef(Bool(true))
	l := NewList(vs, r)
	vs.WriteValue(l)

	vs.Commit(vs.Root(), vs.Root())
}

type badVersionStore struct {
	chunks.ChunkStore
}

func (b *badVersionStore) Version() string {
	return "BAD"
}
