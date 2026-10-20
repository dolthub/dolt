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
	_ "embed"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/dolt_system_variables.snapshot
var doltSystemVariablesSnapshot string

// TestDoltSystemVariablesSnapshot pins every definition Dolt registers in an
// engine, and which of them DoltSystemVariableNames reports.
func TestDoltSystemVariablesSnapshot(t *testing.T) {
	require.Equal(t, doltSystemVariablesSnapshot, describeDoltSystemVariables(t))
}

func describeDoltSystemVariables(t *testing.T) string {
	recorder := &nameRecorder{}
	AddDoltSystemVariablesToRegistry(recorder)
	names := slices.Compact(slices.Sorted(slices.Values(recorder.names)))
	listed := make(map[string]bool)
	for _, name := range DoltSystemVariableNames() {
		require.False(t, listed[name], "duplicate name %s", name)
		listed[name] = true
	}

	vars := NewEngineVariables()
	var b strings.Builder
	for _, name := range names {
		def, _, ok := vars.System.GetGlobal(name)
		require.True(t, ok, name)
		m, ok := def.(*sql.MysqlSystemVariable)
		require.True(t, ok, "%s: %T", name, def)
		fmt.Fprintf(&b, "%s listed=%t scope=%v dynamic=%t setvar=%t type=%#v default=%T(%#v) notify=%t valuefn=%t\n",
			m.Name, listed[name], m.Scope.Type, m.Dynamic, m.SetVarHintApplies, m.Type,
			m.Default, m.Default, m.NotifyChanged != nil, m.ValueFunction != nil)
		delete(listed, name)
	}
	require.Empty(t, listed, "names reported but not registered")
	return b.String()
}

type nameRecorder struct {
	sql.SystemVariableRegistry
	names []string
}

func (r *nameRecorder) AddSystemVariables(defs []sql.SystemVariable) {
	for _, def := range defs {
		r.names = append(r.names, def.GetName())
	}
}
