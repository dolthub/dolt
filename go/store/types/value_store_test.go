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
	vs.skipWriteCaching = true
	assert.Nil(vs.ReadValue(context.Background(), mustHash(s.Hash(vs.Format())))) // nil
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
	vs.skipWriteCaching = true

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
	vs.skipWriteCaching = true
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
	_, err := vs.ReadValue(context.Background(), mustHash(vals[0].Hash(vs.Format())))
	require.NoError(t, err)

	// Get one Value into vs's pendingPuts
	three := Float(3)
	vals = append(vals, three)
	_, err = vs.WriteValue(context.Background(), three)
	require.NoError(t, err)
	hashes = append(hashes, mustHash(three.Hash(vs.Format())))

	// Add one Value to request that's not in vs
	hashes = append(hashes, mustHash(Bool(false).Hash(vs.Format())))

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
		assert.True(v.Equals(found[mustHash(v.Hash(vs.Format()))]))
	}
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

func TestErrorIfDangling(t *testing.T) {
	t.Skip("WriteValue errors with dangling ref error")
	vs := newTestValueStore()
	vs.skipWriteCaching = true

	r, err := NewRef(Bool(true), vs.Format())
	require.NoError(t, err)
	l, err := NewList(context.Background(), vs, r)
	require.NoError(t, err)
	_, err = vs.WriteValue(context.Background(), l)
	require.NoError(t, err)

	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.Error(t, err)
}

func TestGC(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	vs := newTestValueStore()
	vs.skipWriteCaching = true
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

	ok, err = vs.Commit(ctx, h1, h1)
	require.NoError(t, err)
	assert.True(ok)

	v1, err := vs.ReadValue(ctx, h1) // non-nil
	require.NoError(t, err)
	assert.NotNil(v1)
	v2, err := vs.ReadValue(ctx, h2) // non-nil
	require.NoError(t, err)
	assert.NotNil(v2)

	err = vs.GC(ctx, GCModeDefault, hash.HashSet{}, hash.HashSet{}, nil)
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
