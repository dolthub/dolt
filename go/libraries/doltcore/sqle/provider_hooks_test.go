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
	"errors"
	"path/filepath"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func TestEvictProviderGitRemoteIsReached(t *testing.T) {
	p, err := NewDoltDatabaseProvider(env.DefaultInitBranch, filesys.EmptyInMemFS(""), sql.EngineOverrides{})
	require.NoError(t, err)
	p.SetRemoteDialer(env.NewGRPCDialProvider())
	saved := evictProviderGitRemote
	t.Cleanup(func() { evictProviderGitRemote = saved })
	refused := errors.New("refused")
	var keys []string
	evictProviderGitRemote = func(got *DoltDatabaseProvider, _ context.Context, key string, _ map[string]interface{}) error {
		require.Same(t, p, got)
		keys = append(keys, key)
		return refused
	}

	dir := t.TempDir()
	r := env.NewRemote("origin", "git+file://"+filepath.ToSlash(filepath.Join(dir, "missing.git")), map[string]string{dbfactory.GitCacheRootParam: dir})
	_, err = p.GetRemoteDB(context.Background(), types.Format_DOLT, r)
	require.ErrorIs(t, err, refused)
	require.Len(t, keys, 1)
}

func TestOpenPushOnWriteRemoteIsReached(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()
	require.NoError(t, dEnv.AddRemote(env.NewRemote("origin", "file://"+filepath.ToSlash(t.TempDir()), nil)))
	refused := errors.New("refused")
	saved := openPushOnWriteRemote
	t.Cleanup(func() { openPushOnWriteRemote = saved })
	var opened []string
	openPushOnWriteRemote = func(_ context.Context, r env.Remote, got *env.DoltEnv) (*doltdb.DoltDB, error) {
		require.Same(t, dEnv, got)
		opened = append(opened, r.Name)
		return nil, refused
	}

	_, err := getDestinationDb(ctx, dEnv, "origin")
	require.ErrorIs(t, err, refused)
	require.Equal(t, []string{"origin"}, opened)
}
