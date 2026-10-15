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
	conf := config.NewMapConfig(map[string]string{"private_custom": "persisted", "unknown_private": "ignored"})
	loaded, missing, err := SystemVariablesInConfigWithRegistry(a.System, conf)
	require.NoError(t, err)
	require.Equal(t, []string{"unknown_private"}, missing)
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

func TestPersistedDefinitionsSkipUnknownKeys(t *testing.T) {
	vars := variables.NewEngineVariables()
	vars.System.AddSystemVariables([]sql.SystemVariable{&sql.MysqlSystemVariable{
		Name: "private_known", Scope: sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic: true, Type: types.NewSystemStringType("private_known"), Default: "original",
	}})
	conf := config.NewMapConfig(map[string]string{
		"private_known":   "persisted",
		"private_unknown": "persisted",
	})
	loaded, missing, err := SystemVariablesInConfigWithRegistry(vars.System, conf)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	require.NotNil(t, loaded[0])
	require.Equal(t, "private_known", loaded[0].GetName())
	require.Equal(t, []string{"private_unknown"}, missing)
}
