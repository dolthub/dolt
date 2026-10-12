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
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func TestSqlEngineCloseStopsBinlogManager(t *testing.T) {
	root, home := t.TempDir(), t.TempDir()
	// Bubble exit fails if any goroutine created by the engine remains blocked.
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		fs, err := filesys.LocalFS.WithWorkingDir(root)
		require.NoError(t, err)
		rootEnv := env.LoadWithoutDB(ctx, func() (string, error) { return home, nil }, fs, doltdb.LocalDirDoltDB, "test")
		mrEnv, err := env.MultiEnvForDirectory(ctx, fs, rootEnv)
		require.NoError(t, err)
		for range 3 {
			se, err := NewSqlEngine(ctx, mrEnv, &SqlEngineConfig{ServerUser: "root", ServerHost: "localhost", Autocommit: true})
			require.NoError(t, err)
			require.NoError(t, se.Close())
		}
		// Failed construction must also release the manager it already created.
		_, err = NewSqlEngine(ctx, mrEnv, &SqlEngineConfig{PrivFilePath: root})
		require.Error(t, err)
		synctest.Wait()
	})
}
