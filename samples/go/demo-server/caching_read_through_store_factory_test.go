// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/testify/assert"
)

func TestCaching(t *testing.T) {
	assert := assert.New(t)

	tFactory := chunks.NewTestStoreFactory()
	factory := &cachingReadThroughStoreFactory{chunks.NewMemoryStore(), tFactory}
	defer factory.Shutter()

	chunk := chunks.NewChunk([]byte("abc"))
	store1 := factory.CreateStore("ns")
	tStore := tFactory.Stores["ns"]

	store1.Put(chunk)
	assert.Equal(1, tStore.Writes)

	assert.Equal(0, tStore.Hases)
	assert.True(store1.Has(chunk.Hash()))
	assert.Equal(0, tStore.Hases)

	assert.Equal(chunk, store1.Get(chunk.Hash()))
	assert.Equal(0, tStore.Reads)

	assert.True(store1.Has(chunk.Hash()))
	assert.Equal(0, tStore.Hases)

	// And cache should work across all stores vended by the factory.
	store2 := factory.CreateStore("ns")
	assert.Equal(chunk, store2.Get(chunk.Hash()))
	assert.Equal(0, tStore.Reads)
	assert.True(store2.Has(chunk.Hash()))
	assert.Equal(0, tStore.Hases)
}
