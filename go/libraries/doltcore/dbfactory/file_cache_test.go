package dbfactory

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
			ChunkJournalParam:        struct{}{},
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

