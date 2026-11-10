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
	"path/filepath"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func TestEvictCachedGitRemoteIsReached(t *testing.T) {
	p, err := NewDoltDatabaseProvider(env.DefaultInitBranch, filesys.EmptyInMemFS(""), sql.EngineOverrides{})
	require.NoError(t, err)
	p.SetRemoteDialer(env.NewGRPCDialProvider())
	saved := evictCachedGitRemote
	t.Cleanup(func() { evictCachedGitRemote = saved })
	var keys []string
	evictCachedGitRemote = func(got *DoltDatabaseProvider, _ context.Context, key string, _ map[string]interface{}) {
		require.Same(t, p, got)
		keys = append(keys, key)
	}

	dir := t.TempDir()
	r := env.NewRemote("origin", "git+file://"+filepath.ToSlash(filepath.Join(dir, "missing.git")), map[string]string{dbfactory.GitCacheRootParam: dir})
	_, err = p.GetRemoteDB(context.Background(), types.Format_DOLT, r)
	require.Error(t, err)
	require.Len(t, keys, 1)
}
