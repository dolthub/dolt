// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestValueReadWriteRead(t *testing.T) {
	assert := assert.New(t)

	s := String("hello")
	vs := newTestValueStore()
	assert.Nil(vs.ReadValue(context.Background(), mustHash(s.Hash(Format_7_18)))) // nil
	h := mustRef(vs.WriteValue(context.Background(), s)).TargetHash()
	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)
	v, err := vs.ReadValue(context.Background(), h) // non-nil
	require.NoError(t, err)
	if assert.NotNil(v) {
		assert.True(s.Equals(v), "%s != %s", mustString(EncodedValue(context.Background(), s)), mustString(EncodedValue(context.Background(), v)))
	}
}

func TestReadWriteCache(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.TestStorage{}
	ts := storage.NewView()
	vs := NewValueStore(ts)

	var v Value = Bool(true)
	r, err := vs.WriteValue(context.Background(), v)
	require.NoError(t, err)
	assert.NotEqual(hash.Hash{}, r.TargetHash())
	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)
	assert.Equal(1, ts.Writes())

	v, err = vs.ReadValue(context.Background(), r.TargetHash())
	require.NoError(t, err)
	assert.True(v.Equals(Bool(true)))
	assert.Equal(1, ts.Reads())

	v, err = vs.ReadValue(context.Background(), r.TargetHash())
	require.NoError(t, err)
	assert.True(v.Equals(Bool(true)))
	assert.Equal(1, ts.Reads())
}

func TestValueReadMany(t *testing.T) {
	assert := assert.New(t)

	vals := ValueSlice{String("hello"), Bool(true), Float(42)}
	vs := newTestValueStore()
	hashes := hash.HashSlice{}
	for _, v := range vals {
		h := mustRef(vs.WriteValue(context.Background(), v)).TargetHash()
		hashes = append(hashes, h)
		rt, err := vs.Root(context.Background())
		require.NoError(t, err)
		_, err = vs.Commit(context.Background(), rt, rt)
		require.NoError(t, err)
	}

	// Get one Value into vs's Value cache
	_, err := vs.ReadValue(context.Background(), mustHash(vals[0].Hash(Format_7_18)))
	require.NoError(t, err)

	// Get one Value into vs's pendingPuts
	three := Float(3)
	vals = append(vals, three)
	_, err = vs.WriteValue(context.Background(), three)
	require.NoError(t, err)
	hashes = append(hashes, mustHash(three.Hash(Format_7_18)))

	// Add one Value to request that's not in vs
	hashes = append(hashes, mustHash(Bool(false).Hash(Format_7_18)))

	found := map[hash.Hash]Value{}
	readValues, err := vs.ReadManyValues(context.Background(), hashes)
	require.NoError(t, err)

	for i, v := range readValues {
		if v != nil {
			found[hashes[i]] = v
		}
	}

	assert.Len(found, len(vals))
	for _, v := range vals {
		assert.True(v.Equals(found[mustHash(v.Hash(Format_7_18))]))
	}
}

func TestValueWriteFlush(t *testing.T) {
	assert := assert.New(t)

	vals := ValueSlice{String("hello"), Bool(true), Float(42)}
	vs := newTestValueStore()
	hashes := hash.HashSet{}
	for _, v := range vals {
		hashes.Insert(mustRef(vs.WriteValue(context.Background(), v)).TargetHash())
	}
	assert.NotZero(vs.bufferedChunkSize)

	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)
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

func (cbs *checkingChunkStore) Put(ctx context.Context, c chunks.Chunk) error {
	if cbs.a.NotZero(len(cbs.expectedOrder), "Unexpected Put of %s", c.Hash()) {
		cbs.a.Equal(cbs.expectedOrder[0], c.Hash())
		cbs.expectedOrder = cbs.expectedOrder[1:]
	}
	return cbs.ChunkStore.Put(context.Background(), c)
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
	n := Float(42)
	sr, err := vs.WriteValue(context.Background(), s)
	require.NoError(t, err)
	nr, err := vs.WriteValue(context.Background(), n)
	require.NoError(t, err)
	ccs.expect(sr, nr)
	ml, err := NewList(context.Background(), vs, sr, nr)
	require.NoError(t, err)

	b, err := NewEmptyBlob(vs)
	require.NoError(t, err)
	br, err := vs.WriteValue(context.Background(), b)
	require.NoError(t, err)
	mlr, err := vs.WriteValue(context.Background(), ml)
	require.NoError(t, err)
	ccs.expect(br, mlr)
	ml1, err := NewList(context.Background(), vs, br, mlr)
	require.NoError(t, err)

	f := Bool(false)
	fr, err := vs.WriteValue(context.Background(), f)
	require.NoError(t, err)
	ccs.expect(fr)
	ml2, err := NewList(context.Background(), vs, fr)
	require.NoError(t, err)

	ml1r, err := vs.WriteValue(context.Background(), ml1)
	require.NoError(t, err)
	ml2r, err := vs.WriteValue(context.Background(), ml2)
	require.NoError(t, err)
	ccs.expect(ml1r, ml2r)
	l, err := NewList(context.Background(), vs, ml1r, ml2r)
	require.NoError(t, err)

	r, err := vs.WriteValue(context.Background(), l)
	require.NoError(t, err)
	ccs.expect(r)
	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)
}

func TestFlushOverSize(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.TestStorage{}
	ccs := &checkingChunkStore{storage.NewView(), assert, nil}
	vs := newValueStoreWithCacheAndPending(ccs, 0, 30)

	s := String("oy")
	sr, err := vs.WriteValue(context.Background(), s)
	require.NoError(t, err)
	ccs.expect(sr)
	NewList(context.Background(), vs, sr) // will write the root chunk
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
	sr, err := vs.WriteValue(context.Background(), S)
	require.NoError(t, err)
	ccs.expect(sr)

	ML, err := NewList(context.Background(), vs, sr)
	require.NoError(t, err)
	mlr, err := vs.WriteValue(context.Background(), ML)
	require.NoError(t, err)
	ccs.expect(mlr)

	L, err := NewList(context.Background(), vs, mlr)
	require.NoError(t, err)
	lr, err := vs.WriteValue(context.Background(), L)
	require.NoError(t, err)
	ccs.expect(lr)

	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)

	assert.Zero(len(vs.bufferedChunks))

	ST, err := NewStruct(Format_7_18, "", StructData{"r": mlr})
	require.NoError(t, err)
	str, err := vs.WriteValue(context.Background(), ST) // ST into bufferedChunks
	require.NoError(t, err)
	_, err = vs.WriteValue(context.Background(), S) // S into bufferedChunks
	require.NoError(t, err)
	_, err = vs.WriteValue(context.Background(), ML) // ML into bufferedChunks AND withBufferedChunks
	require.NoError(t, err)

	// At this point, ValueStore believes ST is a standalone chunk, and that ML -> S
	// So, it'll look at ML, the one parent it knows about, first and write its child (S). Then, it'll write ML, and then it'll flush the remaining buffered chunks, which is just ST.
	ccs.expect(sr, mlr, str)
	rt, err = vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)
}

func TestPanicOnBadVersion(t *testing.T) {
	storage := &chunks.MemoryStorage{}
	t.Run("Read", func(t *testing.T) {
		cvs := NewValueStore(&badVersionStore{ChunkStore: storage.NewView()})
		assert.Panics(t, func() {
			cvs.ReadValue(context.Background(), hash.Hash{})
		})
	})
	t.Run("Write", func(t *testing.T) {
		cvs := NewValueStore(&badVersionStore{ChunkStore: storage.NewView()})
		assert.Panics(t, func() {
			b, err := NewEmptyBlob(cvs)
			require.NoError(t, err)
			_, err = cvs.WriteValue(context.Background(), b)
			require.NoError(t, err)

			rt, err := cvs.Root(context.Background())
			require.NoError(t, err)
			_, err = cvs.Commit(context.Background(), rt, rt)
			require.NoError(t, err)
		})
	})
}

func TestPanicIfDangling(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	r, err := NewRef(Bool(true), Format_7_18)
	require.NoError(t, err)
	l, err := NewList(context.Background(), vs, r)
	require.NoError(t, err)
	_, err = vs.WriteValue(context.Background(), l)
	require.NoError(t, err)

	assert.Panics(func() {
		rt, err := vs.Root(context.Background())
		require.NoError(t, err)
		_, err = vs.Commit(context.Background(), rt, rt)
		require.NoError(t, err)
	})
}

func TestSkipEnforceCompleteness(t *testing.T) {
	vs := newTestValueStore()
	vs.SetEnforceCompleteness(false)

	r, err := NewRef(Bool(true), Format_7_18)
	require.NoError(t, err)
	l, err := NewList(context.Background(), vs, r)
	require.NoError(t, err)
	_, err = vs.WriteValue(context.Background(), l)
	require.NoError(t, err)

	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)
}

func TestGC(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	vs := newTestValueStore()
	r1 := mustRef(vs.WriteValue(ctx, String("committed")))
	r2 := mustRef(vs.WriteValue(ctx, String("unreferenced")))
	set1 := mustSet(NewSet(ctx, vs, r1))
	set2 := mustSet(NewSet(ctx, vs, r2))

	h1 := mustRef(vs.WriteValue(ctx, set1)).TargetHash()

	rt, err := vs.Root(ctx)
	require.NoError(t, err)
	ok, err := vs.Commit(ctx, h1, rt)
	require.NoError(t, err)
	assert.True(ok)
	h2 := mustRef(vs.WriteValue(ctx, set2)).TargetHash()

	v1, err := vs.ReadValue(ctx, h1) // non-nil
	require.NoError(t, err)
	assert.NotNil(v1)
	v2, err := vs.ReadValue(ctx, h2) // non-nil
	require.NoError(t, err)
	assert.NotNil(v2)

	err = vs.GC(ctx)
	require.NoError(t, err)

	v1, err = vs.ReadValue(ctx, h1) // non-nil
	require.NoError(t, err)
	assert.NotNil(v1)
	v2, err = vs.ReadValue(ctx, h2) // nil
	require.NoError(t, err)
	assert.Nil(v2)
}

type badVersionStore struct {
	chunks.ChunkStore
}

func (b *badVersionStore) Version() string {
	return "BAD"
}
