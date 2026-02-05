// Copyright 2026 Dolthub, Inc.
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

package dbfactory

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

func TestFileFactory_CreateDB_SingletonCacheAndBypass(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default

	t.Run("default uses singleton cache", func(t *testing.T) {
		root := t.TempDir()
		nomsDir := filepath.Join(root, "noms")
		require.NoError(t, os.MkdirAll(nomsDir, 0o755))

		urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)

		db1, _, _, err := CreateDB(ctx, nbf, urlStr, map[string]interface{}{ChunkJournalParam: struct{}{}})
		require.NoError(t, err)
		t.Cleanup(func() { _ = CloseAllLocalDatabases() })

		db2, _, _, err := CreateDB(ctx, nbf, urlStr, map[string]interface{}{ChunkJournalParam: struct{}{}})
		require.NoError(t, err)

		require.True(t, db1 == db2, "expected singleton cache to return same DB instance")
	})

	t.Run("DisableSingletonCacheParam bypasses singleton cache", func(t *testing.T) {
		root := t.TempDir()
		nomsDir := filepath.Join(root, "noms")
		require.NoError(t, os.MkdirAll(nomsDir, 0o755))

		urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)
		params := map[string]interface{}{
			ChunkJournalParam:          struct{}{},
			DisableSingletonCacheParam: struct{}{},
		}

		db1, _, _, err := CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db1.Close() })

		db2, _, _, err := CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db2.Close() })

		require.True(t, db1 != db2, "expected bypass mode to return different DB instances")
	})
}

func TestFileFactory_CreateDB_FailOnJournalLockTimeoutParam(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default

	root := t.TempDir()
	nomsDir := filepath.Join(root, "noms")
	require.NoError(t, os.MkdirAll(nomsDir, 0o755))

	urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)
	params := map[string]interface{}{
		ChunkJournalParam:             struct{}{},
		DisableSingletonCacheParam:    struct{}{},
		FailOnJournalLockTimeoutParam: struct{}{},
	}

	// First open takes the journal manifest lock and holds it until closed.
	db1, _, _, err := CreateDB(ctx, nbf, urlStr, params)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db1.Close() })

	// Second open should fail fast when the fail-on-timeout flag is honored.
	_, _, _, err = CreateDB(ctx, nbf, urlStr, params)
	require.Error(t, err)
	require.ErrorIs(t, err, nbs.ErrDatabaseLocked)
}

func TestFileFactory_CreateDB_OpenReadOnlyParam_DoesNotHoldExclusiveLock(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default

	root := t.TempDir()
	nomsDir := filepath.Join(root, "noms")
	require.NoError(t, os.MkdirAll(nomsDir, 0o755))

	urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)

	// First open takes the journal manifest lock and holds it until closed.
	rwParams := map[string]interface{}{
		ChunkJournalParam:          struct{}{},
		DisableSingletonCacheParam: struct{}{},
	}
	db1, _, _, err := CreateDB(ctx, nbf, urlStr, rwParams)
	require.NoError(t, err)

	// Read-only open should succeed while db1 holds the lock.
	roParams := map[string]interface{}{
		ChunkJournalParam:          struct{}{},
		DisableSingletonCacheParam: struct{}{},
		OpenReadOnlyParam:          struct{}{},
	}
	db2, _, _, err := CreateDB(ctx, nbf, urlStr, roParams)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	// Closing the writer should release the lock; a new writer open should succeed even while db2 is still open
	// if db2 did not acquire/hold the exclusive lock.
	require.NoError(t, db1.Close())

	db3, _, _, err := CreateDB(ctx, nbf, urlStr, rwParams)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db3.Close() })
}

func TestFileFactory_CreateDB_OpenReadOnlyParam_WorksWithoutOtherParams(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default

	root := t.TempDir()
	nomsDir := filepath.Join(root, "noms")
	require.NoError(t, os.MkdirAll(nomsDir, 0o755))

	urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)
	u, err := url.Parse(urlStr)
	require.NoError(t, err)

	// Open a writer that holds the exclusive journal manifest lock.
	db1, _, _, err := CreateDB(ctx, nbf, urlStr, map[string]interface{}{
		ChunkJournalParam: struct{}{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db1.Close() })
	require.EqualValues(t, chunks.ExclusiveAccessMode_Exclusive, datas.ChunkStoreFromDatabase(db1).AccessMode())

	// NOTE: CreateDB uses an in-process singleton cache keyed by URL path and does not currently
	// include params in the cache key. To ensure this test exercises the parameter plumbing,
	// remove the writer from the singleton cache while keeping it open (and holding the lock).
	require.NoError(t, DeleteFromSingletonCache(u.Path, false))

	// Now open a second handle using only open_read_only (no fail-fast, no cache-bypass).
	db2, _, _, err := CreateDB(ctx, nbf, urlStr, map[string]interface{}{
		ChunkJournalParam: struct{}{},
		OpenReadOnlyParam: struct{}{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	require.EqualValues(t, chunks.ExclusiveAccessMode_ReadOnly, datas.ChunkStoreFromDatabase(db2).AccessMode())
}
