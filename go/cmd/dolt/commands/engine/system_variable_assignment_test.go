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

package engine

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

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
