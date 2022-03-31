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

func TestPersistingChunkStoreEmpty(t *testing.T) {
	mt := newMemTable(testMemTableSize)
	ccs := newPersistingChunkSource(context.Background(), mt, nil, newFakeTablePersister(&noopQuotaProvider{}), make(chan struct{}, 1), &Stats{})

	h, err := ccs.hash()
	require.NoError(t, err)
	assert.Equal(t, addr{}, h)
	assert.Zero(t, mustUint32(ccs.count()))
}

type pausingFakeTablePersister struct {
	tablePersister
	trigger <-chan struct{}
}

func (ftp pausingFakeTablePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	<-ftp.trigger
	return ftp.tablePersister.Persist(context.Background(), mt, haver, stats)
}

func TestPersistingChunkStore(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	trigger := make(chan struct{})
	ccs := newPersistingChunkSource(context.Background(), mt, nil, pausingFakeTablePersister{newFakeTablePersister(&noopQuotaProvider{}), trigger}, make(chan struct{}, 1), &Stats{})

	assertChunksInReader(testChunks, ccs, assert)
	assert.EqualValues(mustUint32(mt.count()), mustUint32(ccs.getReader().count()))
	close(trigger)

	h, err := ccs.hash()
	assert.NoError(err)
	assert.NotEqual(addr{}, h)
	assert.EqualValues(len(testChunks), mustUint32(ccs.count()))
	assertChunksInReader(testChunks, ccs, assert)

	assert.Nil(ccs.mt)

	newChunk := []byte("additional")
	mt.addChunk(computeAddr(newChunk), newChunk)
	assert.NotEqual(mustUint32(mt.count()), mustUint32(ccs.count()))
	assert.False(ccs.has(computeAddr(newChunk)))
}
