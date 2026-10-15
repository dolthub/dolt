package engine

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

type legacyRegistry struct{ sql.SystemVariableRegistry }

func TestApplySystemVariablesUsesOwningContext(t *testing.T) {
	vars := sqle.NewEngineVariables()
	ctx := sql.NewContextWithVariables(context.Background(), vars)
	require.NoError(t, applySystemVariables(ctx, SystemVariables{"max_connections": int64(321)}))
	_, got, ok := vars.System.GetGlobal("max_connections")
	require.True(t, ok)
	require.EqualValues(t, 321, got)
	var callbackOwner *sql.EngineVariables
	var callbackValue any
	vars.System.AddSystemVariables([]sql.SystemVariable{&sql.MysqlSystemVariable{
		Name: "assignment_owner_probe", Scope: sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic: true, Type: types.NewSystemStringType("assignment_owner_probe"), Default: "",
		NotifyChanged: func(callbackCtx *sql.Context, _ sql.SystemVariableScope, _ sql.SystemVarValue) error {
			callbackOwner = callbackCtx.EngineVariables()
			_, callbackValue, _ = callbackCtx.SystemVariables().GetGlobal("max_connections")
			return nil
		},
	}})
	require.NoError(t, applySystemVariables(ctx, SystemVariables{"assignment_owner_probe": "set"}))
	require.Same(t, vars, callbackOwner)
	require.EqualValues(t, 321, callbackValue)
}

func TestApplySystemVariablesRejectsMissingContextAssignment(t *testing.T) {
	vars := sqle.NewEngineVariables()
	vars.System = legacyRegistry{vars.System}
	ctx := sql.NewContextWithVariables(context.Background(), vars)
	require.ErrorContains(t, applySystemVariables(ctx, SystemVariables{"max_connections": int64(321)}), "does not support context-aware assignment")
	require.ErrorContains(t, applySystemVariables(nil, SystemVariables{"max_connections": int64(321)}), "requires an owning context")
}
