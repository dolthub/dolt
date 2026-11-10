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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func TestCloneLoadParamsReachTheClone(t *testing.T) {
	ctx := context.Background()
	src, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, doltdb.InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	defer src.Close()
	saved := cloneLoadParams
	t.Cleanup(func() { cloneLoadParams = saved })
	var got *doltdb.DoltDB
	cloneLoadParams = func(_ context.Context, db *doltdb.DoltDB) (map[string]interface{}, error) {
		got = db
		return map[string]interface{}{"cloned": "1"}, nil
	}

	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)
	var configured map[string]interface{}
	dEnv, err := EnvForCloneOf(ctx, src, env.NoRemote, "cloned", fs, "test", func() (string, error) { return fs.TempDir(), nil }, func(dEnv *env.DoltEnv) {
		configured = dEnv.DBLoadParams
	})
	require.NoError(t, err)
	defer dEnv.Close()
	require.Same(t, src, got)
	require.Nil(t, configured)
	require.Equal(t, "1", dEnv.DBLoadParams["cloned"])
}
