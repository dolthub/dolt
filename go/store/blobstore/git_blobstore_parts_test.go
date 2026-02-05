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

package blobstore

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func TestStagePartReachable_Idempotent(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	runner, err := git.NewRunner(repo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)

	partOID, err := api.HashObject(ctx, strings.NewReader("part-bytes"))
	require.NoError(t, err)

	_, indexFile, cleanup, err := newTempIndex()
	require.NoError(t, err)
	defer cleanup()

	require.NoError(t, api.ReadTreeEmpty(ctx, indexFile))

	path1, err := stagePartReachable(ctx, api, indexFile, partOID)
	require.NoError(t, err)
	path2, err := stagePartReachable(ctx, api, indexFile, partOID)
	require.NoError(t, err)
	require.Equal(t, path1, path2)

	treeOID, err := api.WriteTree(ctx, indexFile)
	require.NoError(t, err)

	commitOID, err := api.CommitTree(ctx, treeOID, nil, "stage part reachable test", &git.Identity{Name: "t", Email: "t@t"})
	require.NoError(t, err)

	// Verify the staged path resolves to the part blob in the committed tree.
	got, err := api.ResolvePathBlob(ctx, commitOID, path1)
	require.NoError(t, err)
	require.Equal(t, partOID, got)
}
