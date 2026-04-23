// Copyright 2021 Dolthub, Inc.
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
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

func TestCommitHooksNoErrors(t *testing.T) {
	ctx := sql.NewEmptyContext()
	dEnv, err := CreateEnvWithSeedData()
	require.NoError(t, err)
	defer dEnv.Close()

	sql.SystemVariables.SetGlobal(ctx, dsess.SkipReplicationErrors, true)
	sql.SystemVariables.SetGlobal(ctx, dsess.ReplicateToRemote, "unknown")
	hooks, _, err := GetCommitHooks(context.Background(), "", dEnv, &buffer.Buffer{})
	assert.NoError(t, err)
	if len(hooks) < 1 {
		t.Error("failed to produce noop hook")
	} else {
		switch h := hooks[0].(type) {
		case *LogHook:
		default:
			t.Errorf("expected LogHook, found: %s", h)
		}
	}
}

func TestCommitHooksBackgroundThreadsUniqueNames(t *testing.T) {
	ctx := sql.NewEmptyContext()
	dEnv, err := CreateEnvWithSeedData()
	require.NoError(t, err)
	defer dEnv.Close()

	dest := t.TempDir()
	t.Cleanup(func() {
		dbfactory.DeleteFromSingletonCache(dest, true)
	})

	err = dEnv.AddRemote(env.NewRemote("origin", "file://"+dest, map[string]string{
		dbfactory.DisableSingletonCacheParam: "true",
	}))
	require.NoError(t, err)

	sql.SystemVariables.SetGlobal(ctx, dsess.ReplicateToRemote, "origin")
	sql.SystemVariables.SetGlobal(ctx, dsess.AsyncReplication, "true")
	_, runThreads, err := GetCommitHooks(context.Background(), "[dbname]", dEnv, &buffer.Buffer{})
	require.NoError(t, err)
	require.NotNil(t, runThreads)
	bt := &testBackgroundThreads{make(map[string]func(context.Context))}
	runThreads(bt, func(ctx context.Context) (*sql.Context, error) {
		return sql.NewEmptyContext(), nil
	})
	assert.Len(t, bt.threads, 2)
	for k := range bt.threads {
		assert.True(t, strings.HasSuffix(k, "[dbname]"), "the requested suffix appears in the registered thread name")
	}
}

type testBackgroundThreads struct {
	threads map[string]func(context.Context)
}

func (t *testBackgroundThreads) Add(name string, f func(context.Context)) error {
	t.threads[name] = f
	return nil
}

func TestReplicationBranches(t *testing.T) {
	tests := []struct {
		remote      []string
		local       []string
		expToDelete []string
	}{
		{
			remote:      []string{"main", "feature1", "feature2"},
			local:       []string{"main", "feature1", "feature2"},
			expToDelete: []string{},
		},
		{
			remote:      []string{"main", "feature1"},
			local:       []string{"main", "feature1", "feature2"},
			expToDelete: []string{"feature2"},
		},
		{
			remote:      []string{"main", "feature1", "feature2"},
			local:       []string{"main", "feature1"},
			expToDelete: []string{},
		},
		{
			remote:      []string{"main", "feature1"},
			local:       []string{"main", "feature2"},
			expToDelete: []string{"feature2"},
		},
		{
			remote:      []string{"main", "feature1", "feature2", "feature3"},
			local:       []string{"feature4", "feature5", "feature6", "feature7", "feature8", "feature9"},
			expToDelete: []string{"feature4", "feature5", "feature6", "feature7", "feature8", "feature9"},
		},
		{
			remote:      []string{"main", "new1", "a1"},
			local:       []string{"main", "a1"},
			expToDelete: []string{},
		},
	}

	for _, tt := range tests {
		remoteRefs := make([]doltdb.RefWithHash, len(tt.remote))
		for i := range tt.remote {
			remoteRefs[i] = doltdb.RefWithHash{Ref: ref.NewBranchRef(tt.remote[i])}
		}
		localRefs := make([]doltdb.RefWithHash, len(tt.local))
		for i := range tt.local {
			localRefs[i] = doltdb.RefWithHash{Ref: ref.NewBranchRef(tt.local[i])}
		}
		diff := refsToDelete(remoteRefs, localRefs)
		diffNames := make([]string, len(diff))
		for i := range diff {
			diffNames[i] = diff[i].Ref.GetPath()
		}
		assert.Equal(t, tt.expToDelete, diffNames)
	}
}
