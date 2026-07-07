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

package actions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func TestEnvForCloneMarksDatabaseInProgress(t *testing.T) {
	// A process killed after EnvForClone must leave the directory marked, so callers that die before the
	// clone content is complete leave a directory that is ignored rather than served.
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)
	hdp := func() (string, error) { return fs.TempDir(), nil }

	dEnv, err := EnvForClone(context.Background(), types.Format_DOLT, env.NoRemote, "cloned", fs, "test", hdp)
	require.NoError(t, err)
	defer dEnv.Close()

	require.True(t, dbfactory.IsDatabaseInProgress(dEnv.FS), "EnvForClone must mark the directory before clone content arrives")

	require.NoError(t, dbfactory.ClearDatabaseInProgress(dEnv.FS))
	require.False(t, dbfactory.IsDatabaseInProgress(dEnv.FS))
}
