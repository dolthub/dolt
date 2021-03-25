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

func TestDynamoParseAppendixIfExists(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// Manifest must exist to create/update appendices
	mc := makeContents("mannyLock", "mannyRoot", []tableSpec{{computeAddr([]byte("daMan")), 355}})
	_, err := mm.Update(context.Background(), addr{}, mc, stats, nil)
	require.NoError(t, err)

	updater, ok := mm.(appendixUpdater)
	require.True(t, ok)

	exists, _, err := updater.ParseAppendixIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.False(exists)

	// Simulate another process writing a appendix (with an old Noms version).
	newLock := computeAddr([]byte("locker"))
	newRoot := hash.Of([]byte("new root"))
	tableName := hash.Of([]byte("table1"))
	app := appendix{
		lock:  newLock[:],
		root:  newRoot[:],
		vers:  "0",
		specs: tableName.String() + ":" + "0",
	}
	ddb.updateRecord(db, app)
	assert.Equal(app, ddb.getRecordAppendix(db))

	// ParseAppendixIfExists should now reflect the appendix written above.
	exists, contents, err := updater.ParseAppendixIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.True(exists)
	assert.Equal("0", contents.vers)
	assert.Equal(newLock, contents.lock)
	assert.Equal(newRoot, contents.root)
	if assert.Len(contents.specs, 1) {
		assert.Equal(tableName.String(), contents.specs[0].name.String())
		assert.Equal(uint32(0), contents.specs[0].chunkCount)
	}
}

func TestDynamoAppendixUpdate(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// Manifest must exist to create/update appendices
	mc := makeContents("mannyLock", "mannyRoot", []tableSpec{{computeAddr([]byte("daMan")), 355}})
	_, err := mm.Update(context.Background(), addr{}, mc, stats, nil)
	require.NoError(t, err)

	updater, ok := mm.(appendixUpdater)
	require.True(t, ok)

	// First, test winning the race against another process.
	contents := makeContents("locker", "nuroot", []tableSpec{{computeAddr([]byte("a")), 3}})
	upstream, err := updater.UpdateAppendix(context.Background(), addr{}, contents, stats, func() error {
		// This should fail to get the lock, and therefore _not_ clobber the appendix. So the Update should succeed.
		lock := computeAddr([]byte("nolock"))
		newRoot2 := hash.Of([]byte("noroot"))
		app := appendix{
			lock:  lock[:],
			root:  newRoot2[:],
			vers:  constants.NomsVersion,
			specs: "",
		}
		ddb.updateRecord(db, app)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)

	// Now, test the case where the optimistic lock fails, and someone else updated the root since last we checked.
	rejected := makeContents("locker 2", "new root 2", nil)
	upstream, err = updater.UpdateAppendix(context.Background(), addr{}, rejected, stats, nil)
	require.NoError(t, err)

	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)
	upstream, err = updater.UpdateAppendix(context.Background(), upstream.lock, rejected, stats, nil)
	require.NoError(t, err)
	assert.Equal(rejected.lock, upstream.lock)
	assert.Equal(rejected.root, upstream.root)
	assert.Empty(upstream.specs)

	// Now, test the case where the optimistic lock fails because someone else updated only the tables since last we checked
	jerkLock := computeAddr([]byte("jerk"))
	tableName := computeAddr([]byte("table1"))
	app := appendix{
		lock:  jerkLock[:],
		root:  upstream.root[:],
		vers:  constants.NomsVersion,
		specs: tableName.String() + ":1",
	}
	ddb.updateRecord(db, app)
	assert.Equal(app, ddb.getRecordAppendix(db))

	newContents3 := makeContents("locker 3", "new root 3", nil)
	upstream, err = updater.UpdateAppendix(context.Background(), upstream.lock, newContents3, stats, nil)
	require.NoError(t, err)
	assert.Equal(jerkLock, upstream.lock)
	assert.Equal(rejected.root, upstream.root)
	assert.Equal([]tableSpec{{tableName, 1}}, upstream.specs)
}

func TestDynamoAppendixUpdateEmpty(t *testing.T) {
	assert := assert.New(t)
	mm, _ := makeDynamoManifestFake(t)
	stats := &Stats{}

	// Manifest must exist to create/update appendices
	mc := makeContents("mannyLock", "mannyRoot", []tableSpec{{computeAddr([]byte("daMan")), 355}})
	_, err := mm.Update(context.Background(), addr{}, mc, stats, nil)
	require.NoError(t, err)

	updater, ok := mm.(appendixUpdater)
	require.True(t, ok)

	contents := manifestContents{vers: constants.NomsVersion, lock: computeAddr([]byte{0x01})}
	upstream, err := updater.UpdateAppendix(context.Background(), addr{}, contents, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)
}
