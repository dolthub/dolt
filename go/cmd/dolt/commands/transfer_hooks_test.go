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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func TestOpenBareRepositoryIsReached(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs, err := filesys.LocalFilesysWithWorkingDir(dir)
	require.NoError(t, err)
	dEnv := env.Load(ctx, func() (string, error) { return t.TempDir(), nil }, fs, doltdb.LocalDirDoltDB, "test")
	saved := openBareRepository
	t.Cleanup(func() { openBareRepository = saved })
	var opened string
	openBareRepository = func(_ context.Context, path string) (remotesrv.DBCache, error) {
		opened = path
		return &singletonDBCache{}, nil
	}

	cache, _, err := resolveDatabaseChunkStore(ctx, dEnv)
	require.NoError(t, err)
	require.Equal(t, &singletonDBCache{}, cache)
	abs, err := fs.Abs(".")
	require.NoError(t, err)
	require.Equal(t, abs, opened)
}
