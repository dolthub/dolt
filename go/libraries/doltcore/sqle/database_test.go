// Copyright 2019-2020 Dolthub, Inc.
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

package sqle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

func testKeyFunc(t *testing.T, keyFunc func(string) (bool, string), testVal string, expectedIsKey bool, expectedDBName string) {
	isKey, dbName := keyFunc(testVal)
	assert.Equal(t, expectedIsKey, isKey)
	assert.Equal(t, expectedDBName, dbName)
}

func TestIsKeyFuncs(t *testing.T) {
	testKeyFunc(t, dsess.IsHeadKey, "", false, "")
	testKeyFunc(t, dsess.IsWorkingKey, "", false, "")
	testKeyFunc(t, dsess.IsHeadKey, "dolt_head", true, "dolt")
	testKeyFunc(t, dsess.IsWorkingKey, "dolt_head", false, "")
	testKeyFunc(t, dsess.IsHeadKey, "dolt_working", false, "")
	testKeyFunc(t, dsess.IsWorkingKey, "dolt_working", true, "dolt")
}

func TestNeedsToReloadEvents(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	tmpDir, err := dEnv.TempTableFilesDir()
	require.NoError(t, err)
	opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}

	timestamp := time.Now().Truncate(time.Minute).UTC()

	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), opts)
	require.NoError(t, err)

	_, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)

	var token any

	t.Run("empty schema table doesn't need to be reloaded", func(t *testing.T) {
		needsReload, err := db.NeedsToReloadEvents(sqlCtx, token)
		require.NoError(t, err)
		assert.False(t, needsReload)
	})

	eventDefn := `CREATE EVENT testEvent
ON SCHEDULE
    EVERY 1 DAY
    STARTS now()
DO
BEGIN
    CALL archive_order_history(DATE_SUB(CURDATE(), INTERVAL 1 YEAR));
END`

	err = db.addFragToSchemasTable(sqlCtx, "event", "testEvent", eventDefn, timestamp, nil)
	require.NoError(t, err)

	t.Run("events need to be reloaded after addition", func(t *testing.T) {
		needsReload, err := db.NeedsToReloadEvents(sqlCtx, token)
		require.NoError(t, err)
		assert.True(t, needsReload)
	})

	_, token, err = db.GetEvents(sqlCtx)
	require.NoError(t, err)

	t.Run("events do not need to be reloaded after no change", func(t *testing.T) {
		needsReload, err := db.NeedsToReloadEvents(sqlCtx, token)
		require.NoError(t, err)
		assert.False(t, needsReload)
	})

	err = db.dropFragFromSchemasTable(sqlCtx, "event", "testEvent", nil)
	require.NoError(t, err)

	t.Run("events need to be reloaded after dropping one", func(t *testing.T) {
		needsReload, err := db.NeedsToReloadEvents(sqlCtx, token)
		require.NoError(t, err)
		assert.True(t, needsReload)
	})

	_, token, err = db.GetEvents(sqlCtx)
	require.NoError(t, err)

	t.Run("events do not need to be reloaded after no change", func(t *testing.T) {
		needsReload, err := db.NeedsToReloadEvents(sqlCtx, token)
		require.NoError(t, err)
		assert.False(t, needsReload)
	})
}
