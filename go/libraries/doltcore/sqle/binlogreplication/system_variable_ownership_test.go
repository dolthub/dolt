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

package binlogreplication

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestBinlogVariablesUseOwningRegistry(t *testing.T) {
	a, b := variables.NewEngineVariables(), variables.NewEngineVariables()
	ctxA := sql.NewContextWithVariables(context.Background(), a)
	ctxB := sql.NewContextWithVariables(context.Background(), b)
	require.NoError(t, sqle.AssignSystemVariables(ctxA, map[string]any{
		"server_uuid":   "11111111-1111-1111-1111-111111111111",
		"server_id":     uint32(11),
		"gtid_executed": "11111111-1111-1111-1111-111111111111:1",
	}))
	require.NoError(t, sqle.AssignSystemVariables(ctxB, map[string]any{
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

	lmA, lmB := &logManager{variables: a.System}, &logManager{variables: b.System}
	require.NoError(t, lmA.initializeAvailableGtids())
	require.NoError(t, lmB.initializeAvailableGtids())
	require.Equal(t, "11111111-1111-1111-1111-111111111111:1", lmA.availableGtids.String())
	require.Empty(t, lmB.availableGtids.String())
}
