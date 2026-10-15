package commands

import (
	"context"
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestRestrictLoadFileUsesCliEngineRegistry(t *testing.T) {
	a, b := sqle.NewEngineVariables(), sqle.NewEngineVariables()
	ctxA := sql.NewContextWithVariables(context.Background(), a)
	_, beforeB, _ := b.System.GetGlobal("secure_file_priv")
	require.NoError(t, restrictLoadFileToWorkingDirectory(ctxA))
	wd, err := os.Getwd()
	require.NoError(t, err)
	_, afterA, _ := a.System.GetGlobal("secure_file_priv")
	_, afterB, _ := b.System.GetGlobal("secure_file_priv")
	require.Equal(t, wd, afterA)
	require.Equal(t, beforeB, afterB)
}

type legacySystemVariableRegistry struct{ sql.SystemVariableRegistry }

func TestRestrictLoadFileRejectsRegistryWithoutContextAssignment(t *testing.T) {
	vars := sqle.NewEngineVariables()
	vars.System = legacySystemVariableRegistry{vars.System}
	ctx := sql.NewContextWithVariables(context.Background(), vars)
	require.ErrorContains(t, restrictLoadFileToWorkingDirectory(ctx), "does not support context-aware assignment")
}
