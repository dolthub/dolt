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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testChunks = [][]byte{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")}

func TestTableSetPrependEmpty(t *testing.T) {
	ts := newFakeTableSet(&noopQuotaProvider{}).Prepend(context.Background(), newMemTable(testMemTableSize), &Stats{})
	specs, err := ts.ToSpecs()
	require.NoError(t, err)
	assert.Empty(t, specs)
}

func TestTableSetPrepend(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet(&noopQuotaProvider{})
	specs, err := ts.ToSpecs()
	require.NoError(t, err)
	assert.Empty(specs)
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	firstSpecs, err := ts.ToSpecs()
	require.NoError(t, err)
	assert.Len(firstSpecs, 1)

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	secondSpecs, err := ts.ToSpecs()
	require.NoError(t, err)
	assert.Len(secondSpecs, 2)
	assert.Equal(firstSpecs, secondSpecs[1:])
}

func TestTableSetToSpecsExcludesEmptyTable(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet(&noopQuotaProvider{})
	specs, err := ts.ToSpecs()
	require.NoError(t, err)
	assert.Empty(specs)
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	mt = newMemTable(testMemTableSize)
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	specs, err = ts.ToSpecs()
	require.NoError(t, err)
	assert.Len(specs, 2)
}

func TestTableSetFlattenExcludesEmptyTable(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet(&noopQuotaProvider{})
	specs, err := ts.ToSpecs()
	require.NoError(t, err)
	assert.Empty(specs)
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	mt = newMemTable(testMemTableSize)
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	ts, err = ts.Flatten(context.Background())
	require.NoError(t, err)
	assert.EqualValues(ts.Size(), 2)
}

func TestTableSetExtract(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet(&noopQuotaProvider{})
	specs, err := ts.ToSpecs()
	require.NoError(t, err)
	assert.Empty(specs)

	// Put in one table
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	// Put in a second
	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	chunkChan := make(chan extractRecord)
	go func() {
		defer close(chunkChan)
		err := ts.extract(context.Background(), chunkChan)

		require.NoError(t, err)
	}()
	i := 0
	for rec := range chunkChan {
		a := computeAddr(testChunks[i])
		assert.NotNil(rec.data, "Nothing for", a)
		assert.Equal(testChunks[i], rec.data, "Item %d: %s != %s", i, string(testChunks[i]), string(rec.data))
		assert.Equal(a, rec.a)
		i++
	}
}

func persist(t *testing.T, p tablePersister, chunks ...[]byte) {
	for _, c := range chunks {
		mt := newMemTable(testMemTableSize)
		mt.addChunk(computeAddr(c), c)
		_, err := p.Persist(context.Background(), mt, nil, &Stats{})
		require.NoError(t, err)
	}
}

func TestTableSetRebase(t *testing.T) {
	assert := assert.New(t)
	q := NewUnlimitedMemQuotaProvider()
	defer func() {
		require.EqualValues(t, 0, q.Usage())
	}()
	persister := newFakeTablePersister(q)

	insert := func(ts tableSet, chunks ...[]byte) tableSet {
		for _, c := range chunks {
			mt := newMemTable(testMemTableSize)
			mt.addChunk(computeAddr(c), c)
			ts = ts.Prepend(context.Background(), mt, &Stats{})
		}
		return ts
	}

	fullTS := newTableSet(persister, q)
	defer func() {
		require.NoError(t, fullTS.Close())
	}()
	specs, err := fullTS.ToSpecs()
	require.NoError(t, err)
	assert.Empty(specs)
	fullTS = insert(fullTS, testChunks...)
	fullTS, err = fullTS.Flatten(context.Background())
	require.NoError(t, err)

	ts := newTableSet(persister, q)
	ts = insert(ts, testChunks[0])
	assert.Equal(1, ts.Size())
	ts, err = ts.Flatten(context.Background())
	require.NoError(t, err)
	ts = insert(ts, []byte("novel"))

	specs, err = fullTS.ToSpecs()
	require.NoError(t, err)
	ts2, err := ts.Rebase(context.Background(), specs, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, ts2.Close())
	}()
	err = ts.Close()
	require.NoError(t, err)
	assert.Equal(4, ts2.Size())
}

func TestTableSetPhysicalLen(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet(&noopQuotaProvider{})
	specs, err := ts.ToSpecs()
	require.NoError(t, err)
	assert.Empty(specs)
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(context.Background(), mt, &Stats{})

	assert.True(mustUint64(ts.physicalLen()) > indexSize(mustUint32(ts.count())))
}

func TestTableSetClosesOpenedChunkSourcesOnErr(t *testing.T) {
	q := NewUnlimitedMemQuotaProvider()
	p := newFakeTablePersister(q)
	persist(t, p, testChunks...)

	var mem uint64 = 0
	var sources []addr
	for addr := range p.sources {
		sources = append(sources, addr)
		mem += indexMemSize(1)
	}

	idx := rand.Intn(len(testChunks))
	addrToFail := sources[idx]
	p.sourcesToFail[addrToFail] = true

	var specs []tableSpec
	for _, addr := range sources {
		specs = append(specs, tableSpec{addr, 1})
	}

	ts := tableSet{p: p, q: q, rl: make(chan struct{}, 1)}
	_, err := ts.Rebase(context.Background(), specs, &Stats{})
	require.Error(t, err)

	for _ = range p.opened {
		mem -= indexMemSize(1)
	}
	require.EqualValues(t, mem, q.Usage())
}
