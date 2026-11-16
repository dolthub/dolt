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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

func TestFileFactory_CreateDB_SingletonCacheAndBypass(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_DOLT

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

	t.Run("closed cache entry is replaced", func(t *testing.T) {
		root := t.TempDir()
		nomsDir := filepath.Join(root, "noms")
		require.NoError(t, os.MkdirAll(nomsDir, 0o755))

		urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)
		params := map[string]interface{}{ChunkJournalParam: struct{}{}}

		db1, _, _, err := CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
		t.Cleanup(func() { _ = CloseAllLocalDatabases() })
		require.NoError(t, db1.Close())

		db2, _, _, err := CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
		require.True(t, db1 != db2, "expected a closed store to be replaced")
		_, err = datas.ChunkStoreFromDatabase(db2).Root(ctx)
		require.NoError(t, err)

		db3, _, _, err := CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
		require.True(t, db2 == db3, "expected the replacement to be cached")
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

func TestFileFactory_CreateDB_PanicOnSingletonHit(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_DOLT
	params := map[string]interface{}{ChunkJournalParam: struct{}{}}

	nomsURL := func(t *testing.T) string {
		nomsDir := filepath.Join(t.TempDir(), "noms")
		require.NoError(t, os.MkdirAll(nomsDir, 0o755))
		return earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)
	}

	t.Run("panics on a hit and names the path", func(t *testing.T) {
		t.Setenv(dconfig.EnvTestPanicOnSingletonHit, "1")
		urlStr := nomsURL(t)

		db, _, _, err := CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
		t.Cleanup(func() { _ = CloseAllLocalDatabases() })

		// CreateDB keys the cache by earl.Parse's path, which differs from url.Parse's on Windows.
		urlObj, err := earl.Parse(urlStr)
		require.NoError(t, err)
		require.PanicsWithValue(t, "singleton cache hit opening "+urlObj.Path, func() {
			_, _, _, _ = CreateDB(ctx, nbf, urlStr, params)
		})

		// The panic releases the cache's lock.
		t.Setenv(dconfig.EnvTestPanicOnSingletonHit, "")
		hit, _, _, err := CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
		require.True(t, db == hit)
	})

	t.Run("replacing a closed entry does not panic", func(t *testing.T) {
		t.Setenv(dconfig.EnvTestPanicOnSingletonHit, "1")
		urlStr := nomsURL(t)

		db, _, _, err := CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
		t.Cleanup(func() { _ = CloseAllLocalDatabases() })
		require.NoError(t, db.Close())

		_, _, _, err = CreateDB(ctx, nbf, urlStr, params)
		require.NoError(t, err)
	})

	t.Run("DisableSingletonCacheParam does not panic", func(t *testing.T) {
		t.Setenv(dconfig.EnvTestPanicOnSingletonHit, "1")
		urlStr := nomsURL(t)
		uncached := map[string]interface{}{
			ChunkJournalParam:          struct{}{},
			DisableSingletonCacheParam: struct{}{},
		}

		db1, _, _, err := CreateDB(ctx, nbf, urlStr, uncached)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db1.Close() })

		db2, _, _, err := CreateDB(ctx, nbf, urlStr, uncached)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db2.Close() })
	})
}

func TestFileFactory_CreateDB_FailOnJournalLockTimeoutParam(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_DOLT

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
