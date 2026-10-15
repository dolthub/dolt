package enginetest

import (
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestDoltHarnessEngineVariables(t *testing.T) {
	vars := sqle.NewEngineVariables()
	h := newDoltHarness(t).WithEngineVariables(vars)
	defer h.Close()
	_, err := h.NewEngine(t)
	require.NoError(t, err)
	require.Same(t, vars, h.Engine().Variables())
	require.Same(t, vars, h.NewContext().EngineVariables())
	require.Same(t, vars, h.NewContextWithClient(h.Session().Client()).EngineVariables())
	require.Same(t, vars, h.NewSession().EngineVariables())

	// Engines the harness builds outside NewEngine own the same bundle.
	db := h.NewDatabases("mydb")[0]
	require.Same(t, vars, h.Engine().Variables())
	enginetest.RunQueryWithContext(t, h.Engine(), h, enginetest.NewContext(h), "CREATE TABLE t (i INT PRIMARY KEY)")
	require.NoError(t, h.SnapshotTable(db.(sql.VersionedDatabase), "t", "snapshot"))
}
