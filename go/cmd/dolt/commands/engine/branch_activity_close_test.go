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
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func TestSqlEngineCloseStopsBranchActivityTracker(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			root, home := t.TempDir(), t.TempDir()
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				fs, err := filesys.LocalFS.WithWorkingDir(root)
				require.NoError(t, err)
				rootEnv := env.LoadWithoutDB(ctx, func() (string, error) { return home, nil }, fs, doltdb.LocalDirDoltDB, "test")
				mrEnv, err := env.MultiEnvForDirectory(ctx, fs, rootEnv)
				require.NoError(t, err)
				se, err := NewSqlEngine(ctx, mrEnv, &SqlEngineConfig{Autocommit: true, BranchActivityTracking: enabled})
				require.NoError(t, err)
				require.NoError(t, se.Close())
				require.NoError(t, se.Close())
				// Bubble exit requires all engine goroutines to terminate without caller cancellation.
			})
		})
	}
}

func TestSqlEngineFailedConstructionStopsBranchActivityTracker(t *testing.T) {
	root, home := t.TempDir(), t.TempDir()
	privs := filepath.Join(root, "privileges.db")
	// Loading malformed privilege data fails after the tracker has started.
	require.NoError(t, os.WriteFile(privs, []byte{0}, 0600))
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		fs, err := filesys.LocalFS.WithWorkingDir(root)
		require.NoError(t, err)
		rootEnv := env.LoadWithoutDB(ctx, func() (string, error) { return home, nil }, fs, doltdb.LocalDirDoltDB, "test")
		mrEnv, err := env.MultiEnvForDirectory(ctx, fs, rootEnv)
		require.NoError(t, err)
		_, err = NewSqlEngine(ctx, mrEnv, &SqlEngineConfig{PrivFilePath: privs, BranchActivityTracking: true})
		require.ErrorContains(t, err, "ill formatted privileges file")
	})
}
