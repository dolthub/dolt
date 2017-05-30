// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

func TestLocalStoreFactory(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(assert)
	defer os.RemoveAll(dir)

	f := NewLocalStoreFactory(dir, 0, 8)
	stats := &Stats{}

	dbName := "db"
	store := f.CreateStore(dbName)

	c := chunks.NewChunk([]byte{0xff})
	store.Put(c)
	assert.True(store.Commit(c.Hash(), hash.Hash{}))

	dbDir := filepath.Join(dir, dbName)
	exists, contents := newFileManifest(dbDir, newManifestCache(0)).ParseIfExists(stats, nil)
	assert.True(exists)
	assert.Len(contents.specs, 1)

	_, err := os.Stat(filepath.Join(dbDir, contents.specs[0].name.String()))
	assert.NoError(err)
}
