package adapters

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

type mockAdapter struct {
	name string
}

func (m mockAdapter) NewTable(_ *sql.Context, _ string, _ *doltdb.DoltDB, _ *doltdb.WorkingSet, _ env.RootsProvider[*sql.Context]) sql.Table {
	return nil
}

func (m mockAdapter) TableName() string {
	return m.name
}

func TestDoltTableAdapterRegistry(t *testing.T) {
	registry := newDoltTableAdapterRegistry()

	statusAdapter := mockAdapter{name: "status"}
	logAdapter := mockAdapter{name: "log"}

	registry.AddAdapter(doltdb.StatusTableName, statusAdapter, "status")
	registry.AddAdapter(doltdb.LogTableName, logAdapter, "log")

	t.Run("GetAdapter", func(t *testing.T) {
		adapter, ok := registry.GetAdapter("dolt_status")
		require.True(t, ok)
		require.Equal(t, "status", adapter.TableName())

		adapter, ok = registry.GetAdapter("status")
		require.True(t, ok)
		require.Equal(t, "status", adapter.TableName())

		_, ok = registry.GetAdapter("unknown_alias")
		require.False(t, ok)

		_, ok = registry.GetAdapter("dolt_unknown")
		require.False(t, ok)
	})

	t.Run("NormalizeName", func(t *testing.T) {
		normalized := registry.NormalizeName("status")
		require.Equal(t, "dolt_status", normalized)

		normalized = registry.NormalizeName("log")
		require.Equal(t, "dolt_log", normalized)

		normalized = registry.NormalizeName("dolt_status")
		require.Equal(t, "dolt_status", normalized)

		normalized = registry.NormalizeName("unknown_table")
		require.Equal(t, "unknown_table", normalized)
	})
}
