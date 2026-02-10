// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func testAuthor() *git.Identity {
	return &git.Identity{Name: "gitrebase test", Email: "gitrebase@test.invalid"}
}

func newTestGitAPI(t *testing.T, ctx context.Context) (*git.Runner, git.GitAPI) {
	t.Helper()

	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	r, err := git.NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	return r, git.NewGitAPIImpl(r)
}

func mkCommit(t *testing.T, ctx context.Context, api git.GitAPI, parent *git.OID, files map[string][]byte, msg string) git.OID {
	t.Helper()

	_, indexFile, cleanup, err := git.NewTempIndex()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	if parent != nil && parent.String() != "" {
		if err := api.ReadTree(ctx, *parent, indexFile); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
			t.Fatal(err)
		}
	}

	for p, b := range files {
		oid, err := api.HashObject(ctx, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oid, p); err != nil {
			t.Fatal(err)
		}
	}

	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}

	var parentPtr *git.OID
	if parent != nil && parent.String() != "" {
		p := *parent
		parentPtr = &p
	}
	commitOID, err := api.CommitTree(ctx, treeOID, parentPtr, msg, testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	return commitOID
}

func TestMergeRemoteTrackingIntoLocalRef_FastForward(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, api := newTestGitAPI(t, ctx)

	base := mkCommit(t, ctx, api, nil, nil, "base")
	remote := mkCommit(t, ctx, api, &base, map[string][]byte{"r.txt": []byte("remote\n")}, "remote")

	localRef := "refs/dolt/data"
	remoteRef := "refs/dolt/remotes/origin/data"
	if err := api.UpdateRef(ctx, localRef, base, "set local"); err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateRef(ctx, remoteRef, remote, "set remote tracking"); err != nil {
		t.Fatal(err)
	}

	newHead, updated, err := MergeRemoteTrackingIntoLocalRef(ctx, api, localRef, remoteRef, "merge", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatalf("expected updated")
	}
	if newHead != remote {
		t.Fatalf("expected fast-forward to remote head %q, got %q", remote, newHead)
	}
	got, err := api.ResolveRefCommit(ctx, localRef)
	if err != nil {
		t.Fatal(err)
	}
	if got != remote {
		t.Fatalf("local ref mismatch: got %q want %q", got, remote)
	}
}

func TestMergeRemoteTrackingIntoLocalRef_MergeCommit_NoConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, api := newTestGitAPI(t, ctx)

	base := mkCommit(t, ctx, api, nil, nil, "base")
	local := mkCommit(t, ctx, api, &base, map[string][]byte{"local.txt": []byte("l\n")}, "local")
	remote := mkCommit(t, ctx, api, &base, map[string][]byte{"remote.txt": []byte("r\n")}, "remote")

	localRef := "refs/dolt/data"
	remoteRef := "refs/dolt/remotes/origin/data"
	if err := api.UpdateRef(ctx, localRef, local, "set local"); err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateRef(ctx, remoteRef, remote, "set remote tracking"); err != nil {
		t.Fatal(err)
	}

	newHead, updated, err := MergeRemoteTrackingIntoLocalRef(ctx, api, localRef, remoteRef, "merge", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatalf("expected updated")
	}
	if newHead == local || newHead == remote {
		t.Fatalf("expected new merge commit, got %q", newHead)
	}

	// Ensure both paths exist at the merge commit.
	if _, err := api.ResolvePathBlob(ctx, newHead, "local.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := api.ResolvePathBlob(ctx, newHead, "remote.txt"); err != nil {
		t.Fatal(err)
	}

	// Verify parents.
	out, err := r.Run(ctx, git.RunOptions{}, "rev-parse", newHead.String()+"^1")
	if err != nil {
		t.Fatal(err)
	}
	if gotP1 := string(bytes.TrimSpace(out)); gotP1 != local.String() {
		t.Fatalf("parent1 mismatch: got %q want %q", gotP1, local.String())
	}
	out, err = r.Run(ctx, git.RunOptions{}, "rev-parse", newHead.String()+"^2")
	if err != nil {
		t.Fatal(err)
	}
	if gotP2 := string(bytes.TrimSpace(out)); gotP2 != remote.String() {
		t.Fatalf("parent2 mismatch: got %q want %q", gotP2, remote.String())
	}
}

func TestMergeRemoteTrackingIntoLocalRef_Conflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, api := newTestGitAPI(t, ctx)

	base := mkCommit(t, ctx, api, nil, map[string][]byte{"k": []byte("0\n")}, "base")
	local := mkCommit(t, ctx, api, &base, map[string][]byte{"k": []byte("1\n")}, "local")
	remote := mkCommit(t, ctx, api, &base, map[string][]byte{"k": []byte("2\n")}, "remote")

	localRef := "refs/dolt/data"
	remoteRef := "refs/dolt/remotes/origin/data"
	if err := api.UpdateRef(ctx, localRef, local, "set local"); err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateRef(ctx, remoteRef, remote, "set remote tracking"); err != nil {
		t.Fatal(err)
	}

	_, updated, err := MergeRemoteTrackingIntoLocalRef(ctx, api, localRef, remoteRef, "merge", testAuthor())
	if err == nil {
		t.Fatalf("expected conflict error")
	}
	if updated {
		t.Fatalf("expected not updated")
	}
	var ce *git.MergeConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("expected git.MergeConflictError, got %T: %v", err, err)
	}
	found := false
	for _, c := range ce.Conflicts {
		if c == "k" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected conflict on k, got %+v", ce.Conflicts)
	}

	got, err := api.ResolveRefCommit(ctx, localRef)
	if err != nil {
		t.Fatal(err)
	}
	if got != local {
		t.Fatalf("local ref advanced unexpectedly: got %q want %q", got, local)
	}
}

func TestMergeRemoteTrackingIntoLocalRef_ChunkedTreeAtomicConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, api := newTestGitAPI(t, ctx)

	base := mkCommit(t, ctx, api, nil, map[string][]byte{
		"big/0001": []byte("a\n"),
		"big/0002": []byte("b\n"),
	}, "base")
	local := mkCommit(t, ctx, api, &base, map[string][]byte{
		"big/0001": []byte("a1\n"),
	}, "local")
	remote := mkCommit(t, ctx, api, &base, map[string][]byte{
		"big/0002": []byte("b2\n"),
	}, "remote")

	localRef := "refs/dolt/data"
	remoteRef := "refs/dolt/remotes/origin/data"
	if err := api.UpdateRef(ctx, localRef, local, "set local"); err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateRef(ctx, remoteRef, remote, "set remote tracking"); err != nil {
		t.Fatal(err)
	}

	_, _, err := MergeRemoteTrackingIntoLocalRef(ctx, api, localRef, remoteRef, "merge", testAuthor())
	if err == nil {
		t.Fatalf("expected conflict error")
	}
	var ce *git.MergeConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("expected git.MergeConflictError, got %T: %v", err, err)
	}
	found := false
	for _, c := range ce.Conflicts {
		if c == "big" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected conflict on big, got %+v", ce.Conflicts)
	}
}

func TestMergeRemoteTrackingIntoLocalRef_ConflictHook_RemoteWins(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, api := newTestGitAPI(t, ctx)

	base := mkCommit(t, ctx, api, nil, map[string][]byte{"k": []byte("0\n")}, "base")
	local := mkCommit(t, ctx, api, &base, map[string][]byte{"k": []byte("1\n")}, "local")
	remote := mkCommit(t, ctx, api, &base, map[string][]byte{"k": []byte("2\n")}, "remote")

	localRef := "refs/dolt/data"
	remoteRef := "refs/dolt/remotes/origin/data"
	if err := api.UpdateRef(ctx, localRef, local, "set local"); err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateRef(ctx, remoteRef, remote, "set remote tracking"); err != nil {
		t.Fatal(err)
	}

	newHead, updated, err := MergeRemoteTrackingIntoLocalRefWithOptions(ctx, api, localRef, remoteRef, MergeOptions{
		Message:    "merge",
		Author:     testAuthor(),
		OnConflict: ConflictRemoteWins,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatalf("expected updated")
	}
	if newHead != remote {
		t.Fatalf("expected new head to be remote, got %q want %q", newHead, remote)
	}
	got, err := api.ResolveRefCommit(ctx, localRef)
	if err != nil {
		t.Fatal(err)
	}
	if got != remote {
		t.Fatalf("local ref mismatch after remote-wins: got %q want %q", got, remote)
	}
}
