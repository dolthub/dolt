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

package sqle

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

func TestNewEngineVariablesForEnv(t *testing.T) {
	ctx := context.Background()
	dEnv := CreateTestEnv()
	defer dEnv.Close()
	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.FS, dEnv)
	require.NoError(t, err)
	defaultBranch := dsess.DefaultBranchKey(mrEnv.GetFirstDatabase())
	persist := func(name, value string) {
		require.NoError(t, dEnv.Config.WriteableConfig().SetStrings(map[string]string{
			env.SqlServerGlobalsPrefix + "." + name: value,
		}))
	}
	persist("max_connections", "91")
	persist(defaultBranch, "other")

	vars, err := NewEngineVariablesForEnv(mrEnv, nil)
	require.NoError(t, err)
	_, value, ok := vars.System.GetGlobal("max_connections")
	require.True(t, ok)
	require.EqualValues(t, 91, value)
	_, value, ok = vars.System.GetGlobal(defaultBranch)
	require.True(t, ok)
	require.Equal(t, "other", value)

	persist("max_connections", "string")
	vars, err = NewEngineVariablesForEnv(mrEnv, dEnv)
	require.ErrorContains(t, err, "max_connections")
	_, _, ok = vars.System.GetGlobal(defaultBranch)
	require.True(t, ok)
}

func TestDoltSystemVariablesIndependentRegistries(t *testing.T) {
	a, b := NewEngineVariables(), NewEngineVariables()

	effective := make(map[string]sql.SystemVariable)
	for _, list := range [][]sql.SystemVariable{newInlineDoltSystemVariables(), newDoltSystemVariables()} {
		for _, expected := range list {
			effective[expected.GetName()] = expected
		}
	}
	for name, expected := range effective {
		defA, _, ok := a.System.GetGlobal(name)
		require.True(t, ok, name)
		defB, _, ok := b.System.GetGlobal(name)
		require.True(t, ok, name)
		require.NotSame(t, defA, defB, name)
		require.Equal(t, expected, defA, name)
		require.Equal(t, expected, defB, name)
	}

	for _, name := range []string{"binlog_ignore_dbs", "dolt_stats_enabled", "dolt_dont_merge_json", "sql_warnings"} {
		_, _, ok := a.System.GetGlobal(name)
		require.True(t, ok, name)
	}

	defA, _, _ := a.System.GetGlobal("log_bin_branch")
	defB, _, _ := b.System.GetGlobal("log_bin_branch")
	require.NotSame(t, defA, defB)
	defA.SetDefault("other")
	require.Equal(t, "main", defB.GetDefault())

	custom := &sql.MysqlSystemVariable{Name: "test_private_dolt_var", Default: "a"}
	a.System.AddSystemVariables([]sql.SystemVariable{custom})
	_, _, ok := b.System.GetGlobal(custom.Name)
	require.False(t, ok)
}

func TestDynamicHookReadsItsRegistry(t *testing.T) {
	a, b := NewEngineVariables(), NewEngineVariables()
	dEnv, err := CreateEnvWithSeedData()
	require.NoError(t, err)
	defer dEnv.Close()

	hook, _, err := NewDynamicPushOnWriteHookWithRegistry(context.Background(), "", dEnv, &buffer.Buffer{}, a.System)
	require.NoError(t, err)
	ctxB := sql.NewContextWithVariables(context.Background(), b)
	require.NoError(t, b.System.(sql.ContextAwareSystemVariableRegistry).AssignValuesWithContext(ctxB, map[string]interface{}{dsess.ReplicateToRemote: "missing"}))
	_, err = hook.Execute(context.Background(), datas.Dataset{}, nil)
	require.NoError(t, err)
	ctxA := sql.NewContextWithVariables(context.Background(), a)
	require.NoError(t, a.System.(sql.ContextAwareSystemVariableRegistry).AssignValuesWithContext(ctxA, map[string]interface{}{dsess.ReplicateToRemote: "missing"}))
	_, err = hook.Execute(context.Background(), datas.Dataset{}, nil)
	require.Error(t, err)
}
