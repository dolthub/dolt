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
	sessionValue, err := ctxA.Session.GetSessionVariable(ctxA, "secure_file_priv")
	require.NoError(t, err)
	require.Equal(t, wd, sessionValue)
}

type legacySystemVariableRegistry struct{ sql.SystemVariableRegistry }

func TestRestrictLoadFileRejectsRegistryWithoutContextAssignment(t *testing.T) {
	vars := sqle.NewEngineVariables()
	vars.System = legacySystemVariableRegistry{vars.System}
	ctx := sql.NewContextWithVariables(context.Background(), vars)
	require.ErrorContains(t, restrictLoadFileToWorkingDirectory(ctx), "does not support context-aware assignment")
}
