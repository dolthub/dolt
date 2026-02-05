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

package gitrebase

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

const (
	localRef          = "refs/dolt/data"
	remoteTrackingRef = "refs/dolt/remotes/origin/data"
)

func requireGitOnPath(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}
}

func testIdentity() *git.Identity {
	return &git.Identity{Name: "gitrebase test", Email: "gitrebase@test.invalid"}
}

func tempIndexFile(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "index")
}

func commitWithFiles(t *testing.T, ctx context.Context, api git.GitAPI, parent *git.OID, files map[string][]byte, msg string) git.OID {
	t.Helper()

	index := tempIndexFile(t)
	if parent != nil && parent.String() != "" {
		require.NoError(t, api.ReadTree(ctx, *parent, index))
	} else {
		require.NoError(t, api.ReadTreeEmpty(ctx, index))
	}
	for p, data := range files {
		oid, err := api.HashObject(ctx, bytes.NewReader(data))
		require.NoError(t, err)
		require.NoError(t, api.UpdateIndexCacheInfo(ctx, index, "100644", oid, p))
	}
	tree, err := api.WriteTree(ctx, index)
	require.NoError(t, err)
	c, err := api.CommitTree(ctx, tree, parent, msg, testIdentity())
	require.NoError(t, err)
	return c
}

func readPathAtRef(t *testing.T, ctx context.Context, api git.GitAPI, ref string, path string) []byte {
	t.Helper()
	commit, err := api.ResolveRefCommit(ctx, ref)
	require.NoError(t, err)
	blobOID, err := api.ResolvePathBlob(ctx, commit, path)
	require.NoError(t, err)
	rc, err := api.BlobReader(ctx, blobOID)
	require.NoError(t, err)
	defer rc.Close()
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	return b
}

func TestMergeRemoteTrackingIntoLocalRef_FastForward(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	require.NoError(t, err)

	r, err := git.NewRunner(repo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(r)

	base := commitWithFiles(t, ctx, api, nil, map[string][]byte{"base": []byte("base\n")}, "base")
	p := base
	child := commitWithFiles(t, ctx, api, &p, map[string][]byte{"child": []byte("child\n")}, "child")

	require.NoError(t, api.UpdateRef(ctx, localRef, base, "set local"))
	require.NoError(t, api.UpdateRef(ctx, remoteTrackingRef, child, "set remote"))

	newHead, res, err := MergeRemoteTrackingIntoLocalRef(ctx, api, localRef, remoteTrackingRef, testIdentity())
	require.NoError(t, err)
	require.Equal(t, MergeFastForward, res)
	require.Equal(t, child, newHead)

	got, err := api.ResolveRefCommit(ctx, localRef)
	require.NoError(t, err)
	require.Equal(t, child, got)
}

func TestMergeRemoteTrackingIntoLocalRef_MergeNoConflict(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	require.NoError(t, err)

	r, err := git.NewRunner(repo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(r)

	base := commitWithFiles(t, ctx, api, nil, map[string][]byte{"common": []byte("c\n")}, "base")
	p1 := base
	ours := commitWithFiles(t, ctx, api, &p1, map[string][]byte{"ours": []byte("o\n")}, "ours")
	p2 := base
	theirs := commitWithFiles(t, ctx, api, &p2, map[string][]byte{"theirs": []byte("t\n")}, "theirs")

	require.NoError(t, api.UpdateRef(ctx, localRef, ours, "set local"))
	require.NoError(t, api.UpdateRef(ctx, remoteTrackingRef, theirs, "set remote"))

	newHead, res, err := MergeRemoteTrackingIntoLocalRef(ctx, api, localRef, remoteTrackingRef, testIdentity())
	require.NoError(t, err)
	require.Equal(t, MergeMerged, res)
	require.NotEmpty(t, newHead)

	require.Equal(t, []byte("o\n"), readPathAtRef(t, ctx, api, localRef, "ours"))
	require.Equal(t, []byte("t\n"), readPathAtRef(t, ctx, api, localRef, "theirs"))
	require.Equal(t, []byte("c\n"), readPathAtRef(t, ctx, api, localRef, "common"))
}

func TestMergeRemoteTrackingIntoLocalRef_ConflictDoesNotUpdateLocalRef(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	require.NoError(t, err)

	r, err := git.NewRunner(repo.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(r)

	base := commitWithFiles(t, ctx, api, nil, map[string][]byte{"k": []byte("base\n")}, "base")
	p1 := base
	ours := commitWithFiles(t, ctx, api, &p1, map[string][]byte{"k": []byte("ours\n")}, "ours")
	p2 := base
	theirs := commitWithFiles(t, ctx, api, &p2, map[string][]byte{"k": []byte("theirs\n")}, "theirs")

	require.NoError(t, api.UpdateRef(ctx, localRef, ours, "set local"))
	require.NoError(t, api.UpdateRef(ctx, remoteTrackingRef, theirs, "set remote"))

	_, _, err = MergeRemoteTrackingIntoLocalRef(ctx, api, localRef, remoteTrackingRef, testIdentity())
	require.Error(t, err)
	var mce *git.MergeConflictError
	require.True(t, errors.As(err, &mce))

	got, err := api.ResolveRefCommit(ctx, localRef)
	require.NoError(t, err)
	require.Equal(t, ours, got)
	require.Equal(t, []byte("ours\n"), readPathAtRef(t, ctx, api, localRef, "k"))
}
