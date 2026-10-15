package binlogreplication

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/stretchr/testify/require"
)

func TestBinlogVariablesUseOwningRegistry(t *testing.T) {
	a, b := variables.NewEngineVariables(), variables.NewEngineVariables()
	ctxA := sql.NewContextWithVariables(context.Background(), a)
	ctxB := sql.NewContextWithVariables(context.Background(), b)
	require.NoError(t, assignSystemVariables(ctxA, a.System, map[string]any{
		"server_uuid":   "11111111-1111-1111-1111-111111111111",
		"server_id":     uint32(11),
		"gtid_executed": "11111111-1111-1111-1111-111111111111:1",
	}))
	require.NoError(t, assignSystemVariables(ctxB, b.System, map[string]any{
		"server_uuid": "22222222-2222-2222-2222-222222222222",
		"server_id":   uint32(22),
	}))

	idA, err := getServerId(ctxA)
	require.NoError(t, err)
	idB, err := getServerId(ctxB)
	require.NoError(t, err)
	require.Equal(t, uint32(11), idA)
	require.Equal(t, uint32(22), idB)
	uuidA, err := getServerUuid(ctxA)
	require.NoError(t, err)
	uuidB, err := getServerUuid(ctxB)
	require.NoError(t, err)
	require.Equal(t, "11111111-1111-1111-1111-111111111111", uuidA)
	require.Equal(t, "22222222-2222-2222-2222-222222222222", uuidB)
	_, executedB, _ := b.System.GetGlobal("gtid_executed")
	require.NotEqual(t, "11111111-1111-1111-1111-111111111111:1", executedB)
	require.Error(t, assignSystemVariables(ctxB, a.System, map[string]any{"gtid_executed": "changed"}))

	lmA, lmB := &logManager{variables: a.System}, &logManager{variables: b.System}
	require.NoError(t, lmA.initializeAvailableGtids())
	require.NoError(t, lmB.initializeAvailableGtids())
	require.Equal(t, "11111111-1111-1111-1111-111111111111:1", lmA.availableGtids.String())
	require.Empty(t, lmB.availableGtids.String())
}
