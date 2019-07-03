// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/must"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersistingChunkStoreEmpty(t *testing.T) {
	mt := newMemTable(testMemTableSize)
	ccs := newPersistingChunkSource(context.Background(), mt, nil, newFakeTablePersister(), make(chan struct{}, 1), &Stats{})

	h, err := ccs.hash()
	assert.NoError(t, err)
	assert.Equal(t, addr{}, h)
	assert.Zero(t, must.Uint32(ccs.count()))
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
	ccs := newPersistingChunkSource(context.Background(), mt, nil, pausingFakeTablePersister{newFakeTablePersister(), trigger}, make(chan struct{}, 1), &Stats{})

	assertChunksInReader(testChunks, ccs, assert)
	assert.EqualValues(must.Uint32(mt.count()), must.Uint32(ccs.getReader().count()))
	close(trigger)

	h, err := ccs.hash()
	assert.NoError(err)
	assert.NotEqual(addr{}, h)
	assert.EqualValues(len(testChunks), must.Uint32(ccs.count()))
	assertChunksInReader(testChunks, ccs, assert)

	assert.Nil(ccs.mt)

	newChunk := []byte("additional")
	mt.addChunk(computeAddr(newChunk), newChunk)
	assert.NotEqual(must.Uint32(mt.count()), must.Uint32(ccs.count()))
	assert.False(ccs.has(computeAddr(newChunk)))
}
