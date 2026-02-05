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

package git

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func testAuthor() *Identity {
	return &Identity{Name: "gitapi test", Email: "gitapi@test.invalid"}
}

func tempIndexFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "index")
}

func TestGitAPIImpl_HashObject_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	want := []byte("hello dolt\n")
	oid, err := api.HashObject(ctx, bytes.NewReader(want))
	if err != nil {
		t.Fatal(err)
	}
	if oid == "" {
		t.Fatalf("expected non-empty oid")
	}

	typ, err := api.CatFileType(ctx, oid)
	if err != nil {
		t.Fatal(err)
	}
	if typ != "blob" {
		t.Fatalf("expected type blob, got %q", typ)
	}

	rc, err := api.BlobReader(ctx, oid)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("blob mismatch: got %q, want %q", string(got), string(want))
	}
}

func TestGitAPIImpl_HashObject_Empty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	oid, err := api.HashObject(ctx, bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if oid == "" {
		t.Fatalf("expected non-empty oid")
	}

	sz, err := api.BlobSize(ctx, oid)
	if err != nil {
		t.Fatal(err)
	}
	if sz != 0 {
		t.Fatalf("expected size 0, got %d", sz)
	}
}

func TestGitAPIImpl_ResolveRefCommit_Missing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	_, err = api.ResolveRefCommit(ctx, "refs/does/not/exist")
	if err == nil {
		t.Fatalf("expected error")
	}
	var rnf *RefNotFoundError
	if !errors.As(err, &rnf) {
		t.Fatalf("expected RefNotFoundError, got %T: %v", err, err)
	}
}

func TestGitAPIImpl_ResolveRefCommit_Exists(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}
	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := api.CommitTree(ctx, treeOID, nil, "c1", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	ref := "refs/test/resolve-ref"
	if err := api.UpdateRef(ctx, ref, commitOID, "set"); err != nil {
		t.Fatal(err)
	}
	got, err := api.ResolveRefCommit(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got != commitOID {
		t.Fatalf("ref mismatch: got %q, want %q", got, commitOID)
	}
}

func TestGitAPIImpl_WriteTree_FromEmptyIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}

	want := []byte("file contents\n")
	blobOID, err := api.HashObject(ctx, bytes.NewReader(want))
	if err != nil {
		t.Fatal(err)
	}

	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", blobOID, "a/b.txt"); err != nil {
		t.Fatal(err)
	}

	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := api.CommitTree(ctx, treeOID, nil, "test commit", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	gotBlobOID, err := api.ResolvePathBlob(ctx, commitOID, "a/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	rc, err := api.BlobReader(ctx, gotBlobOID)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("blob mismatch: got %q, want %q", string(got), string(want))
	}
}

func TestGitAPIImpl_UpdateIndexCacheInfo_ReplacesExistingEntry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}

	path := "same.txt"

	oid1, err := api.HashObject(ctx, bytes.NewReader([]byte("one\n")))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oid1, path); err != nil {
		t.Fatal(err)
	}

	// Update the same path again; this should succeed and replace the index entry.
	oid2, err := api.HashObject(ctx, bytes.NewReader([]byte("two\n")))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oid2, path); err != nil {
		t.Fatal(err)
	}

	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := api.CommitTree(ctx, treeOID, nil, "replace entry", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	gotBlobOID, err := api.ResolvePathBlob(ctx, commitOID, path)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := api.BlobReader(ctx, gotBlobOID)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("two\n")) {
		t.Fatalf("expected replacement contents %q, got %q", "two\n", string(got))
	}
}

func TestGitAPIImpl_UpdateIndexCacheInfo_FileDirectoryConflictErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}

	oidDirChild, err := api.HashObject(ctx, bytes.NewReader([]byte("child\n")))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oidDirChild, "a/b.txt"); err != nil {
		t.Fatal(err)
	}

	// Now try to stage "a" as a file; git should reject this (file vs directory conflict).
	oidA, err := api.HashObject(ctx, bytes.NewReader([]byte("a\n")))
	if err != nil {
		t.Fatal(err)
	}
	err = api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oidA, "a")
	if err == nil {
		t.Fatalf("expected conflict error staging %q when %q exists", "a", "a/b.txt")
	}

	// Inverse conflict: stage "x" as a file, then try to stage "x/y".
	indexFile2 := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile2); err != nil {
		t.Fatal(err)
	}

	oidX, err := api.HashObject(ctx, bytes.NewReader([]byte("x\n")))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile2, "100644", oidX, "x"); err != nil {
		t.Fatal(err)
	}

	oidXY, err := api.HashObject(ctx, bytes.NewReader([]byte("xy\n")))
	if err != nil {
		t.Fatal(err)
	}
	err = api.UpdateIndexCacheInfo(ctx, indexFile2, "100644", oidXY, "x/y.txt")
	if err == nil {
		t.Fatalf("expected conflict error staging %q when %q exists", "x/y.txt", "x")
	}
}

func TestGitAPIImpl_ReadTree_PreservesExistingPaths(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	// Base commit with one file.
	baseIndex := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, baseIndex); err != nil {
		t.Fatal(err)
	}
	baseContent := []byte("base\n")
	baseBlob, err := api.HashObject(ctx, bytes.NewReader(baseContent))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, baseIndex, "100644", baseBlob, "base.txt"); err != nil {
		t.Fatal(err)
	}
	baseTree, err := api.WriteTree(ctx, baseIndex)
	if err != nil {
		t.Fatal(err)
	}
	baseCommit, err := api.CommitTree(ctx, baseTree, nil, "base", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	// New index starts from base commit's tree, then adds a new path.
	indexFile := tempIndexFile(t)
	if err := api.ReadTree(ctx, baseCommit, indexFile); err != nil {
		t.Fatal(err)
	}

	newContent := []byte("new\n")
	newBlob, err := api.HashObject(ctx, bytes.NewReader(newContent))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", newBlob, "new.txt"); err != nil {
		t.Fatal(err)
	}

	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	parent := baseCommit
	childCommit, err := api.CommitTree(ctx, treeOID, &parent, "child", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	// Verify base path still exists in child commit.
	gotBase, err := api.ResolvePathBlob(ctx, childCommit, "base.txt")
	if err != nil {
		t.Fatal(err)
	}
	rc, err := api.BlobReader(ctx, gotBase)
	if err != nil {
		t.Fatal(err)
	}
	baseGotBytes, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(baseGotBytes, baseContent) {
		t.Fatalf("base blob mismatch: got %q, want %q", string(baseGotBytes), string(baseContent))
	}

	// Verify new path exists in child commit.
	gotNew, err := api.ResolvePathBlob(ctx, childCommit, "new.txt")
	if err != nil {
		t.Fatal(err)
	}
	rc, err = api.BlobReader(ctx, gotNew)
	if err != nil {
		t.Fatal(err)
	}
	newGotBytes, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(newGotBytes, newContent) {
		t.Fatalf("new blob mismatch: got %q, want %q", string(newGotBytes), string(newContent))
	}

	// Verify parent relationship using git rev-parse.
	out, err := r.Run(ctx, RunOptions{}, "rev-parse", childCommit.String()+"^")
	if err != nil {
		t.Fatal(err)
	}
	out = bytes.TrimSpace(out)
	if len(out) == 0 {
		t.Fatalf("rev-parse returned empty output")
	}
	if gotParent := string(out); gotParent != baseCommit.String() {
		t.Fatalf("parent mismatch: got %q, want %q", gotParent, baseCommit.String())
	}
}

func TestGitAPIImpl_FetchRef_UpdatesRemoteTrackingRef(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	origin, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	remoteCommit, err := origin.SetRefToTree(ctx, "refs/dolt/data", map[string][]byte{
		"manifest": []byte("m1\n"),
	}, "seed")
	if err != nil {
		t.Fatal(err)
	}

	local, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(local.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	// Configure a remote pointing at the origin bare repo.
	if _, err := r.Run(ctx, RunOptions{}, "remote", "add", "origin", origin.GitDir); err != nil {
		t.Fatal(err)
	}

	localRef := "refs/dolt/remotes/origin/data"
	if err := api.FetchRef(ctx, "origin", "refs/dolt/data", localRef); err != nil {
		t.Fatal(err)
	}

	got, err := api.ResolveRefCommit(ctx, localRef)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != remoteCommit {
		t.Fatalf("remote-tracking ref mismatch: got %q, want %q", got.String(), remoteCommit)
	}

	// Advance origin and fetch again.
	remoteCommit2, err := origin.SetRefToTree(ctx, "refs/dolt/data", map[string][]byte{
		"manifest": []byte("m2\n"),
	}, "advance")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.FetchRef(ctx, "origin", "refs/dolt/data", localRef); err != nil {
		t.Fatal(err)
	}
	got2, err := api.ResolveRefCommit(ctx, localRef)
	if err != nil {
		t.Fatal(err)
	}
	if got2.String() != remoteCommit2 {
		t.Fatalf("remote-tracking ref mismatch after advance: got %q, want %q", got2.String(), remoteCommit2)
	}
}

func TestGitAPIImpl_FetchRef_MissingRemoteRefDeletesLocalRef(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	origin, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	// Intentionally do not create refs/dolt/data in origin.

	local, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(local.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	// Create the local tracking ref so we can verify it is deleted.
	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}
	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := api.CommitTree(ctx, treeOID, nil, "local seed", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	localRef := "refs/dolt/remotes/origin/data"
	if err := api.UpdateRef(ctx, localRef, commitOID, "seed"); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Run(ctx, RunOptions{}, "remote", "add", "origin", origin.GitDir); err != nil {
		t.Fatal(err)
	}

	// Fetch should treat missing remote ref as "remote empty" and delete localRef.
	if err := api.FetchRef(ctx, "origin", "refs/dolt/data", localRef); err != nil {
		t.Fatal(err)
	}
	_, ok, err := api.TryResolveRefCommit(ctx, localRef)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected %q to be deleted after fetching missing remote ref", localRef)
	}
}

func TestGitAPIImpl_UpdateRef_And_CAS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	// Create two commits on the same tree.
	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}
	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	c1, err := api.CommitTree(ctx, treeOID, nil, "c1", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := api.CommitTree(ctx, treeOID, nil, "c2", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	if c1 == c2 {
		// Extremely unlikely, but avoid flaky tests if git ends up producing identical commits.
		// In that case, ensure c2 differs by changing the message again.
		c2, err = api.CommitTree(ctx, treeOID, nil, "c2b", testAuthor())
		if err != nil {
			t.Fatal(err)
		}
		if c1 == c2 {
			t.Fatalf("expected distinct commit oids")
		}
	}

	ref := "refs/test/gitapi"
	if err := api.UpdateRef(ctx, ref, c1, "set c1"); err != nil {
		t.Fatal(err)
	}
	got, ok, err := api.TryResolveRefCommit(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("expected ref to exist")
	}
	if got != c1 {
		t.Fatalf("ref mismatch: got %q, want %q", got, c1)
	}

	// CAS update from c1 -> c2 succeeds.
	if err := api.UpdateRefCAS(ctx, ref, c2, c1, "cas to c2"); err != nil {
		t.Fatal(err)
	}
	got, ok, err = api.TryResolveRefCommit(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got != c2 {
		t.Fatalf("ref mismatch after cas: ok=%v got=%q want=%q", ok, got, c2)
	}

	// CAS update with stale old oid fails.
	err = api.UpdateRefCAS(ctx, ref, c1, c1, "stale cas") // old oid should be c2 now, not c1.
	if err == nil {
		t.Fatalf("expected cas mismatch error")
	}
	// Ensure ref still points to c2 after failure.
	got, ok, err = api.TryResolveRefCommit(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got != c2 {
		t.Fatalf("ref changed unexpectedly: ok=%v got=%q want=%q", ok, got, c2)
	}
}
