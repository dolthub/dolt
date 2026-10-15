package dsess

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/config"
)

func TestRegistryAwareDatabaseAndPersistedDefinitions(t *testing.T) {
	a, b := variables.NewEngineVariables(), variables.NewEngineVariables()
	DefineSystemVariablesForDBInRegistry(a.System, "private_db")
	_, _, ok := b.System.GetGlobal(HeadKey("private_db"))
	require.False(t, ok)

	for _, vars := range []*sql.EngineVariables{a, b} {
		vars.System.AddSystemVariables([]sql.SystemVariable{&sql.MysqlSystemVariable{
			Name: "private_custom", Scope: sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic: true, Type: types.NewSystemStringType("private_custom"), Default: "original",
		}})
	}
	conf := config.NewMapConfig(map[string]string{"private_custom": "persisted"})
	loaded, missing, err := SystemVariablesInConfigWithRegistry(a.System, conf)
	require.NoError(t, err)
	require.Empty(t, missing)
	require.Len(t, loaded, 1)
	require.Equal(t, "persisted", loaded[0].GetDefault())
	a.System.AddSystemVariables(loaded)
	_, value, _ := a.System.GetGlobal("private_custom")
	require.Equal(t, "persisted", value)
	defB, valueB, _ := b.System.GetGlobal("private_custom")
	require.Equal(t, "original", valueB)
	require.Equal(t, "original", defB.GetDefault())

	defaultSession := DefaultSessionWithVariables(emptyDatabaseProvider(), nil, a)
	require.Same(t, a, defaultSession.EngineVariables())
	detached, err := NewDetachedSessionWithVariables(emptyDatabaseProvider(), config.NewMapConfig(nil), nil, nil, nil, nil, nil, b)
	require.NoError(t, err)
	require.Same(t, b, detached.EngineVariables())
}
