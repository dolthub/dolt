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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testChunks = [][]byte{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")}

func TestTableSetPrependEmpty(t *testing.T) {
	ts := newFakeTableSet().Prepend(context.Background(), newMemTable(testMemTableSize), &Stats{})
	specs, err := ts.ToSpecs()
	require.NoError(t, err)
	assert.Empty(t, specs)
}

func TestTableSetPrepend(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet()
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
	ts := newFakeTableSet()
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
	ts := newFakeTableSet()
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

	ts, err = ts.Flatten()
	require.NoError(t, err)
	assert.EqualValues(ts.Size(), 2)
}

func TestTableSetExtract(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet()
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

func TestTableSetRebase(t *testing.T) {
	assert := assert.New(t)
	persister := newFakeTablePersister()

	insert := func(ts tableSet, chunks ...[]byte) tableSet {
		for _, c := range chunks {
			mt := newMemTable(testMemTableSize)
			mt.addChunk(computeAddr(c), c)
			ts = ts.Prepend(context.Background(), mt, &Stats{})
		}
		return ts
	}
	fullTS := newTableSet(persister)
	specs, err := fullTS.ToSpecs()
	require.NoError(t, err)
	assert.Empty(specs)
	fullTS = insert(fullTS, testChunks...)
	fullTS, err = fullTS.Flatten()
	require.NoError(t, err)

	ts := newTableSet(persister)
	ts = insert(ts, testChunks[0])
	assert.Equal(1, ts.Size())
	ts, err = ts.Flatten()
	require.NoError(t, err)
	ts = insert(ts, []byte("novel"))

	specs, err = fullTS.ToSpecs()
	require.NoError(t, err)
	ts, err = ts.Rebase(context.Background(), specs, nil)
	require.NoError(t, err)
	assert.Equal(4, ts.Size())
}

func TestTableSetPhysicalLen(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet()
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
