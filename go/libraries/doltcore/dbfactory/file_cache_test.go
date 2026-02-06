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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/earl"
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

func TestFileFactory_CreateDB_BlockOnJournalLockParam(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default

	root := t.TempDir()
	nomsDir := filepath.Join(root, "noms")
	require.NoError(t, os.MkdirAll(nomsDir, 0o755))

	urlStr := earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator)
	params := map[string]interface{}{
		ChunkJournalParam:          struct{}{},
		DisableSingletonCacheParam: struct{}{},
		BlockOnJournalLockParam:    struct{}{},
	}

	// First open takes the journal manifest lock and holds it until closed.
	db1, _, _, err := CreateDB(ctx, nbf, urlStr, params)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db1.Close() })

	t.Run("blocks until context deadline exceeded", func(t *testing.T) {
		ctx2, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		t.Cleanup(cancel)

		_, _, _, err = CreateDB(ctx2, nbf, urlStr, params)
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("blocks and succeeds when lock released", func(t *testing.T) {
		ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		t.Cleanup(cancel)

		done := make(chan error, 1)
		go func() {
			db2, _, _, err := CreateDB(ctx2, nbf, urlStr, params)
			if err == nil {
				_ = db2.Close()
			}
			done <- err
		}()

		time.Sleep(50 * time.Millisecond)
		require.NoError(t, db1.Close())

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for second open to complete")
		}
	})
}
