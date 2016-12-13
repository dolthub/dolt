// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestCompactingChunkStoreEmpty(t *testing.T) {
	mt := newMemTable(testMemTableSize)
	ccs := newCompactingChunkSource(mt, nil, newFakeTablePersister(), make(chan struct{}, 1))
	assert.Equal(t, addr{}, ccs.hash())
	assert.Zero(t, ccs.count())
}

type pausingFakeTablePersister struct {
	tablePersister
	trigger <-chan struct{}
}

func (ftp pausingFakeTablePersister) Compact(mt *memTable, haver chunkReader) chunkSource {
	<-ftp.trigger
	return ftp.tablePersister.Compact(mt, haver)
}

func TestCompactingChunkStore(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	trigger := make(chan struct{})
	ccs := newCompactingChunkSource(mt, nil, pausingFakeTablePersister{newFakeTablePersister(), trigger}, make(chan struct{}, 1))

	assertChunksInReader(testChunks, ccs, assert)
	assert.EqualValues(mt.count(), ccs.count())
	close(trigger)

	assert.NotEqual(addr{}, ccs.hash())
	assert.EqualValues(len(testChunks), ccs.count())
	assertChunksInReader(testChunks, ccs, assert)

	assert.Nil(ccs.mt)

	newChunk := []byte("additional")
	mt.addChunk(computeAddr(newChunk), newChunk)
	assert.NotEqual(mt.count(), ccs.count())
	assert.False(ccs.has(computeAddr(newChunk)))
}
