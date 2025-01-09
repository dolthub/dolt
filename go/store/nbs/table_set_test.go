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

package nbs

import (
	"context"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

var testChunks = [][]byte{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")}

var hasManyHasAll = func([]hasRecord) (hash.HashSet, error) {
	return hash.HashSet{}, nil
}

func TestTableSetPrependEmpty(t *testing.T) {
	hasCache, err := lru.New2Q[hash.Hash, struct{}](1024)
	require.NoError(t, err)
	ts, _, err := newFakeTableSet(&UnlimitedQuotaProvider{}).append(context.Background(), newMemTable(testMemTableSize), hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)
	specs, err := ts.toSpecs()
	require.NoError(t, err)
	assert.Empty(t, specs)
}

func TestTableSetPrepend(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet(&UnlimitedQuotaProvider{})
	specs, err := ts.toSpecs()
	defer func() {
		ts.close()
	}()
	require.NoError(t, err)
	assert.Empty(specs)
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	hasCache, err := lru.New2Q[hash.Hash, struct{}](1024)
	require.NoError(t, err)
	ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)

	firstSpecs, err := ts.toSpecs()
	require.NoError(t, err)
	assert.Len(firstSpecs, 1)

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)

	secondSpecs, err := ts.toSpecs()
	require.NoError(t, err)
	assert.Len(secondSpecs, 2)
	assert.Equal(firstSpecs[0], secondSpecs[0])
}

func TestTableSetToSpecsExcludesEmptyTable(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet(&UnlimitedQuotaProvider{})
	defer func() {
		ts.close()
	}()
	specs, err := ts.toSpecs()
	require.NoError(t, err)
	assert.Empty(specs)
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	hasCache, err := lru.New2Q[hash.Hash, struct{}](1024)
	require.NoError(t, err)
	ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)

	mt = newMemTable(testMemTableSize)
	ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)

	specs, err = ts.toSpecs()
	require.NoError(t, err)
	assert.Len(specs, 2)
}

func TestTableSetFlattenExcludesEmptyTable(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet(&UnlimitedQuotaProvider{})
	defer func() {
		ts.close()
	}()
	specs, err := ts.toSpecs()
	require.NoError(t, err)
	assert.Empty(specs)
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	hasCache, err := lru.New2Q[hash.Hash, struct{}](1024)
	require.NoError(t, err)
	ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)

	mt = newMemTable(testMemTableSize)
	ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)

	ts, err = ts.flatten(context.Background())
	require.NoError(t, err)
	assert.EqualValues(ts.Size(), 2)
}

func persist(t *testing.T, p tablePersister, chunks ...[]byte) {
	for _, c := range chunks {
		mt := newMemTable(testMemTableSize)
		mt.addChunk(computeAddr(c), c)
		cs, _, err := p.Persist(context.Background(), mt, nil, nil, &Stats{})
		require.NoError(t, err)
		require.NoError(t, cs.close())
	}
}

func TestTableSetRebase(t *testing.T) {
	assert := assert.New(t)
	q := NewUnlimitedMemQuotaProvider()
	persister := newFakeTablePersister(q)
	hasCache, err := lru.New2Q[hash.Hash, struct{}](1024)
	require.NoError(t, err)

	insert := func(ts tableSet, chunks ...[]byte) tableSet {
		var err error
		for _, c := range chunks {
			mt := newMemTable(testMemTableSize)
			mt.addChunk(computeAddr(c), c)
			ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
			require.NoError(t, err)
		}
		return ts
	}

	fullTS := newTableSet(persister, q)
	defer func() {
		require.NoError(t, fullTS.close())
	}()
	specs, err := fullTS.toSpecs()
	require.NoError(t, err)
	assert.Empty(specs)
	fullTS = insert(fullTS, testChunks...)
	fullTS, err = fullTS.flatten(context.Background())
	require.NoError(t, err)

	ts := newTableSet(persister, q)
	ts = insert(ts, testChunks[0])
	assert.Equal(1, ts.Size())
	ts, err = ts.flatten(context.Background())
	require.NoError(t, err)
	ts = insert(ts, []byte("novel"))

	specs, err = fullTS.toSpecs()
	require.NoError(t, err)
	ts2, err := ts.rebase(context.Background(), specs, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, ts2.close())
	}()
	err = ts.close()
	require.NoError(t, err)
	assert.Equal(4, ts2.Size())
}

func TestTableSetPhysicalLen(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet(&UnlimitedQuotaProvider{})
	defer func() {
		ts.close()
	}()
	specs, err := ts.toSpecs()
	require.NoError(t, err)
	assert.Empty(specs)
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	hasCache, err := lru.New2Q[hash.Hash, struct{}](1024)
	require.NoError(t, err)
	ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts, _, err = ts.append(context.Background(), mt, hasManyHasAll, nil, hasCache, &Stats{})
	require.NoError(t, err)

	assert.True(mustUint64(ts.physicalLen()) > indexSize(mustUint32(ts.count())))
}

func TestTableSetClosesOpenedChunkSourcesOnErr(t *testing.T) {
	q := NewUnlimitedMemQuotaProvider()
	p := newFakeTablePersister(q)
	persist(t, p, testChunks...)

	once := true
	var specs []tableSpec
	for a := range p.sources {
		if once {
			// map iteration is randomized
			p.sourcesToFail[a] = true
		}
		once = false
		specs = append(specs, tableSpec{typeNoms, a, 1})
	}

	ts := newTableSet(p, q)
	ts2, err := ts.rebase(context.Background(), specs, &Stats{})
	require.Error(t, err)

	assert.NoError(t, ts.close())
	assert.NoError(t, ts2.close())
	assert.Equal(t, 0, int(q.Usage()))
}
