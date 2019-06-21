// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/constants"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
)

func TestLocalStoreFactory(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(t)
	defer os.RemoveAll(dir)

	f := NewLocalStoreFactory(dir, 0, 8)
	stats := &Stats{}

	dbName := "db"
	store := f.CreateStore(context.Background(), dbName)

	c := chunks.NewChunk([]byte{0xff})
	store.Put(context.Background(), c)
	assert.True(store.Commit(context.Background(), c.Hash(), hash.Hash{}))

	dbDir := filepath.Join(dir, dbName)
	exists, contents := fileManifest{dbDir}.ParseIfExists(context.Background(), stats, nil)
	assert.True(exists)
	assert.Len(contents.specs, 1)

	_, err := os.Stat(filepath.Join(dbDir, contents.specs[0].name.String()))
	assert.NoError(err)

	// Simulate another process writing a manifest.
	lock := computeAddr([]byte("locker"))
	newRoot := hash.Of([]byte("new root"))
	err = clobberManifest(dbDir, strings.Join([]string{StorageVersion, constants.NomsVersion, lock.String(), newRoot.String(), contents.specs[0].name.String(), "1"}, ":"))
	assert.NoError(err)

	cached := f.CreateStoreFromCache(context.Background(), dbName)
	assert.Equal(c.Hash(), cached.Root(context.Background()))
}
