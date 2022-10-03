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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/buffer"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

func TestCommitHooksNoErrors(t *testing.T) {
	dEnv := CreateEnvWithSeedData(t)
	AddDoltSystemVariables()
	sql.SystemVariables.SetGlobal(dsess.SkipReplicationErrors, true)
	sql.SystemVariables.SetGlobal(dsess.ReplicateToRemote, "unknown")
	bThreads := sql.NewBackgroundThreads()
	hooks, err := GetCommitHooks(context.Background(), bThreads, dEnv, &buffer.Buffer{})
	assert.NoError(t, err)
	if len(hooks) < 1 {
		t.Error("failed to produce noop hook")
	} else {
		switch h := hooks[0].(type) {
		case *doltdb.LogHook:
		default:
			t.Errorf("expected LogHook, found: %s", h)
		}
	}
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
	}

	for _, tt := range tests {
		remoteRefs := make([]ref.DoltRef, len(tt.remote))
		for i := range tt.remote {
			remoteRefs[i] = ref.NewRemoteRef("", tt.remote[i])
		}
		localRefs := make([]ref.DoltRef, len(tt.local))
		for i := range tt.local {
			localRefs[i] = ref.NewBranchRef(tt.local[i])
		}
		diff := branchesToDelete(remoteRefs, localRefs)
		diffNames := make([]string, len(diff))
		for i := range diff {
			diffNames[i] = diff[i].GetPath()
		}
		assert.Equal(t, diffNames, tt.expToDelete)
	}
}
