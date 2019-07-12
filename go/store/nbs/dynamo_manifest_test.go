// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/constants"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
)

const (
	table = "testTable"
	db    = "testDB"
)

func makeDynamoManifestFake(t *testing.T) (mm manifest, ddb *fakeDDB) {
	ddb = makeFakeDDB(t)
	mm = newDynamoManifest(table, db, ddb)
	return
}

func TestDynamoManifestParseIfExists(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	exists, _, err := mm.ParseIfExists(context.Background(), stats, nil)
	assert.NoError(err)
	assert.False(exists)

	// Simulate another process writing a manifest (with an old Noms version).
	newLock := computeAddr([]byte("locker"))
	newRoot := hash.Of([]byte("new root"))
	tableName := hash.Of([]byte("table1"))
	ddb.putRecord(db, newLock[:], newRoot[:], "0", tableName.String()+":"+"0")

	// ParseIfExists should now reflect the manifest written above.
	exists, contents, err := mm.ParseIfExists(context.Background(), stats, nil)
	assert.NoError(err)
	assert.True(exists)
	assert.Equal("0", contents.vers)
	assert.Equal(newLock, contents.lock)
	assert.Equal(newRoot, contents.root)
	if assert.Len(contents.specs, 1) {
		assert.Equal(tableName.String(), contents.specs[0].name.String())
		assert.Equal(uint32(0), contents.specs[0].chunkCount)
	}
}

func makeContents(lock, root string, specs []tableSpec) manifestContents {
	return manifestContents{constants.NomsVersion, computeAddr([]byte(lock)), hash.Of([]byte(root)), specs}
}

func TestDynamoManifestUpdateWontClobberOldVersion(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// Simulate another process having already put old Noms data in dir/.
	lock := computeAddr([]byte("locker"))
	badRoot := hash.Of([]byte("bad root"))
	ddb.putRecord(db, lock[:], badRoot[:], "0", "")

	_, err := mm.Update(context.Background(), lock, manifestContents{vers: constants.NomsVersion}, stats, nil)
	assert.Error(err)
}

func TestDynamoManifestUpdate(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// First, test winning the race against another process.
	contents := makeContents("locker", "nuroot", []tableSpec{{computeAddr([]byte("a")), 3}})
	upstream, err := mm.Update(context.Background(), addr{}, contents, stats, func() error {
		// This should fail to get the lock, and therefore _not_ clobber the manifest. So the Update should succeed.
		lock := computeAddr([]byte("nolock"))
		newRoot2 := hash.Of([]byte("noroot"))
		ddb.putRecord(db, lock[:], newRoot2[:], constants.NomsVersion, "")
		return nil
	})
	assert.NoError(err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)

	// Now, test the case where the optimistic lock fails, and someone else updated the root since last we checked.
	rejected := makeContents("locker 2", "new root 2", nil)
	upstream, err = mm.Update(context.Background(), addr{}, rejected, stats, nil)
	assert.NoError(err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)
	upstream, err = mm.Update(context.Background(), upstream.lock, rejected, stats, nil)
	assert.NoError(err)
	assert.Equal(rejected.lock, upstream.lock)
	assert.Equal(rejected.root, upstream.root)
	assert.Empty(upstream.specs)

	// Now, test the case where the optimistic lock fails because someone else updated only the tables since last we checked
	jerkLock := computeAddr([]byte("jerk"))
	tableName := computeAddr([]byte("table1"))
	ddb.putRecord(db, jerkLock[:], upstream.root[:], constants.NomsVersion, tableName.String()+":1")

	newContents3 := makeContents("locker 3", "new root 3", nil)
	upstream, err = mm.Update(context.Background(), upstream.lock, newContents3, stats, nil)
	assert.NoError(err)
	assert.Equal(jerkLock, upstream.lock)
	assert.Equal(rejected.root, upstream.root)
	assert.Equal([]tableSpec{{tableName, 1}}, upstream.specs)
}

func TestDynamoManifestCaching(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// ParseIfExists should hit persistent storage no matter what
	reads := ddb.numGets
	exists, _, err := mm.ParseIfExists(context.Background(), stats, nil)
	assert.NoError(err)
	assert.False(exists)
	assert.Equal(reads+1, ddb.numGets)

	lock, root := computeAddr([]byte("lock")), hash.Of([]byte("root"))
	ddb.putRecord(db, lock[:], root[:], constants.NomsVersion, "")

	reads = ddb.numGets
	exists, _, err = mm.ParseIfExists(context.Background(), stats, nil)
	assert.NoError(err)
	assert.True(exists)
	assert.Equal(reads+1, ddb.numGets)

	// When failing the optimistic lock, we should hit persistent storage.
	reads = ddb.numGets
	contents := makeContents("lock2", "nuroot", []tableSpec{{computeAddr([]byte("a")), 3}})
	upstream, err := mm.Update(context.Background(), addr{}, contents, stats, nil)
	assert.NoError(err)
	assert.NotEqual(contents.lock, upstream.lock)
	assert.Equal(reads+1, ddb.numGets)

	// Successful update should NOT hit persistent storage.
	reads = ddb.numGets
	upstream, err = mm.Update(context.Background(), upstream.lock, contents, stats, nil)
	assert.NoError(err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(reads, ddb.numGets)
}

func TestDynamoManifestUpdateEmpty(t *testing.T) {
	assert := assert.New(t)
	mm, _ := makeDynamoManifestFake(t)
	stats := &Stats{}

	contents := manifestContents{vers: constants.NomsVersion, lock: computeAddr([]byte{0x01})}
	upstream, err := mm.Update(context.Background(), addr{}, contents, stats, nil)
	assert.NoError(err)
	assert.Equal(contents.lock, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)
}
