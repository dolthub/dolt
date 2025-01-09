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

	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
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
	require.NoError(t, err)
	assert.False(exists)

	// Simulate another process writing a manifest and appendix (with an old Noms version).
	newLock := computeAddr([]byte("locker"))
	newRoot := hash.Of([]byte("new root"))
	tableName := hash.Of([]byte("table1"))
	app := tableName.String() + ":" + "0"
	specsWithAppendix := app + ":" + tableName.String() + ":" + "0"
	ddb.putRecord(db, newLock[:], newRoot[:], "0", specsWithAppendix, app)

	// ParseIfExists should now reflect the manifest written above.
	exists, contents, err := mm.ParseIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.True(exists)
	assert.Equal("0", contents.nbfVers)
	assert.Equal(newLock, contents.lock)
	assert.Equal(newRoot, contents.root)
	if assert.Len(contents.appendix, 1) {
		assert.Equal(tableName.String(), contents.specs[0].hash.String())
		assert.Equal(uint32(0), contents.specs[0].chunkCount)
		assert.Equal(tableName.String(), contents.appendix[0].hash.String())
		assert.Equal(uint32(0), contents.appendix[0].chunkCount)
	}
	if assert.Len(contents.specs, 2) {
		assert.Equal(tableName.String(), contents.specs[1].hash.String())
		assert.Equal(uint32(0), contents.specs[1].chunkCount)
	}
}

func makeContents(lock, root string, specs, appendix []tableSpec) manifestContents {
	return manifestContents{
		nbfVers:  constants.FormatLD1String,
		lock:     computeAddr([]byte(lock)),
		root:     hash.Of([]byte(root)),
		specs:    specs,
		appendix: appendix,
	}
}

func TestDynamoManifestUpdateWontClobberOldVersion(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// Simulate another process having already put old Noms data in dir/.
	lock := computeAddr([]byte("locker"))
	badRoot := hash.Of([]byte("bad root"))
	ddb.putRecord(db, lock[:], badRoot[:], "0", "", "")

	_, err := mm.Update(context.Background(), lock, manifestContents{nbfVers: constants.FormatLD1String}, stats, nil)
	assert.Error(err)
}

func TestDynamoManifestUpdate(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// First, test winning the race against another process.
	contents := makeContents("locker", "nuroot", []tableSpec{{typeNoms, computeAddr([]byte("a")), 3}}, nil)
	upstream, err := mm.Update(context.Background(), hash.Hash{}, contents, stats, func() error {
		// This should fail to get the lock, and therefore _not_ clobber the manifest. So the Update should succeed.
		lock := computeAddr([]byte("nolock"))
		newRoot2 := hash.Of([]byte("noroot"))
		ddb.putRecord(db, lock[:], newRoot2[:], constants.FormatLD1String, "", "")
		return nil
	})
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)

	// Now, test the case where the optimistic lock fails, and someone else updated the root since last we checked.
	rejected := makeContents("locker 2", "new root 2", nil, nil)
	upstream, err = mm.Update(context.Background(), hash.Hash{}, rejected, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)
	upstream, err = mm.Update(context.Background(), upstream.lock, rejected, stats, nil)
	require.NoError(t, err)
	assert.Equal(rejected.lock, upstream.lock)
	assert.Equal(rejected.root, upstream.root)
	assert.Empty(upstream.specs)

	// Now, test the case where the optimistic lock fails because someone else updated only the tables since last we checked
	jerkLock := computeAddr([]byte("jerk"))
	tableName := computeAddr([]byte("table1"))
	ddb.putRecord(db, jerkLock[:], upstream.root[:], constants.FormatLD1String, tableName.String()+":1", "")

	newContents3 := makeContents("locker 3", "new root 3", nil, nil)
	upstream, err = mm.Update(context.Background(), upstream.lock, newContents3, stats, nil)
	require.NoError(t, err)
	assert.Equal(jerkLock, upstream.lock)
	assert.Equal(rejected.root, upstream.root)
	assert.Equal([]tableSpec{{typeNoms, tableName, 1}}, upstream.specs)
}

func TestDynamoManifestUpdateAppendix(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// First, test winning the race against another process.
	specs := []tableSpec{
		{typeNoms, computeAddr([]byte("app-a")), 3},
		{typeNoms, computeAddr([]byte("a")), 3},
	}

	app := []tableSpec{{typeNoms, computeAddr([]byte("app-a")), 3}}
	contents := makeContents("locker", "nuroot", specs, app)

	upstream, err := mm.Update(context.Background(), hash.Hash{}, contents, stats, func() error {
		// This should fail to get the lock, and therefore _not_ clobber the manifest. So the Update should succeed.
		lock := computeAddr([]byte("nolock"))
		newRoot2 := hash.Of([]byte("noroot"))
		ddb.putRecord(db, lock[:], newRoot2[:], constants.FormatLD1String, "", "")
		return nil
	})
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)
	assert.Equal(contents.appendix, upstream.appendix)

	// Now, test the case where the optimistic lock fails, and someone else updated the root since last we checked.
	rejected := makeContents("locker 2", "new root 2", nil, nil)
	upstream, err = mm.Update(context.Background(), hash.Hash{}, rejected, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)
	assert.Equal(contents.appendix, upstream.appendix)

	upstream, err = mm.Update(context.Background(), upstream.lock, rejected, stats, nil)
	require.NoError(t, err)
	assert.Equal(rejected.lock, upstream.lock)
	assert.Equal(rejected.root, upstream.root)
	assert.Empty(upstream.specs)
	assert.Empty(upstream.appendix)

	// Now, test the case where the optimistic lock fails because someone else updated only the tables since last we checked
	jerkLock := computeAddr([]byte("jerk"))
	tableName := computeAddr([]byte("table1"))
	appTableName := computeAddr([]byte("table1-appendix"))
	appStr := appTableName.String() + ":1"
	specsStr := appStr + ":" + tableName.String() + ":1"
	ddb.putRecord(db, jerkLock[:], upstream.root[:], constants.FormatLD1String, specsStr, appStr)

	newContents3 := makeContents("locker 3", "new root 3", nil, nil)
	upstream, err = mm.Update(context.Background(), upstream.lock, newContents3, stats, nil)
	require.NoError(t, err)
	assert.Equal(jerkLock, upstream.lock)
	assert.Equal(rejected.root, upstream.root)
	assert.Equal([]tableSpec{{typeNoms, appTableName, 1}, {typeNoms, tableName, 1}}, upstream.specs)
	assert.Equal([]tableSpec{{typeNoms, appTableName, 1}}, upstream.appendix)
}

func TestDynamoManifestCaching(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// ParseIfExists should hit persistent storage no matter what
	reads := ddb.NumGets()
	exists, _, err := mm.ParseIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.False(exists)
	assert.Equal(reads+1, ddb.NumGets())

	lock, root := computeAddr([]byte("lock")), hash.Of([]byte("root"))
	ddb.putRecord(db, lock[:], root[:], constants.FormatLD1String, "", "")

	reads = ddb.NumGets()
	exists, _, err = mm.ParseIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.True(exists)
	assert.Equal(reads+1, ddb.NumGets())

	// When failing the optimistic lock, we should hit persistent storage.
	reads = ddb.NumGets()
	contents := makeContents("lock2", "nuroot", []tableSpec{{typeNoms, computeAddr([]byte("a")), 3}}, nil)
	upstream, err := mm.Update(context.Background(), hash.Hash{}, contents, stats, nil)
	require.NoError(t, err)
	assert.NotEqual(contents.lock, upstream.lock)
	assert.Equal(reads+1, ddb.NumGets())

	// Successful update should NOT hit persistent storage.
	reads = ddb.NumGets()
	upstream, err = mm.Update(context.Background(), upstream.lock, contents, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(reads, ddb.NumGets())
}

func TestDynamoManifestUpdateEmpty(t *testing.T) {
	assert := assert.New(t)
	mm, _ := makeDynamoManifestFake(t)
	stats := &Stats{}

	contents := manifestContents{nbfVers: constants.FormatLD1String, lock: computeAddr([]byte{0x01})}
	upstream, err := mm.Update(context.Background(), hash.Hash{}, contents, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)
	assert.Empty(upstream.appendix)
}
