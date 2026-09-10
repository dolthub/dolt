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

package env

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
)

func TestRemoveRemoteClearsTrackingBranches(t *testing.T) {
	remotes := concurrentmap.New[string, Remote]()
	remotes.Set("origin", Remote{Name: "origin"})
	branches := concurrentmap.New[string, BranchConfig]()
	branches.Set("tracked", BranchConfig{
		Merge:  ref.MarshalableRef{Ref: ref.NewBranchRef("main")},
		Remote: "origin",
	})
	branches.Set("other", BranchConfig{
		Merge:  ref.MarshalableRef{Ref: ref.NewBranchRef("main")},
		Remote: "upstream",
	})

	repoState := RepoState{Remotes: remotes, Branches: branches}
	repoState.RemoveRemote(Remote{Name: "origin"})

	_, found := repoState.Branches.Get("tracked")
	require.False(t, found)
	_, found = repoState.Branches.Get("other")
	require.True(t, found)
}

func TestRemoveRemoteWithUninitializedBranches(t *testing.T) {
	remotes := concurrentmap.New[string, Remote]()
	remotes.Set("origin", Remote{Name: "origin"})
	repoState := RepoState{Remotes: remotes}

	require.NotPanics(t, func() {
		repoState.RemoveRemote(Remote{Name: "origin"})
	})
	_, found := repoState.Remotes.Get("origin")
	require.False(t, found)
}
