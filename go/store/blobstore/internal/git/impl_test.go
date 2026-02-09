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

func newTestRepo(t *testing.T, ctx context.Context) (*gitrepo.Repo, *Runner, GitAPI) {
	t.Helper()

	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	return repo, r, NewGitAPIImpl(r)
}

func TestGitAPIImpl_HashObject_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, _, api := newTestRepo(t, ctx)

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
	_, _, api := newTestRepo(t, ctx)

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
	_, _, api := newTestRepo(t, ctx)

	_, err := api.ResolveRefCommit(ctx, "refs/does/not/exist")
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
	_, _, api := newTestRepo(t, ctx)

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
	_, _, api := newTestRepo(t, ctx)

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
	_, _, api := newTestRepo(t, ctx)

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
	_, _, api := newTestRepo(t, ctx)

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

func TestGitAPIImpl_ResolvePathObject_BlobAndTree(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, _, api := newTestRepo(t, ctx)

	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}

	blobOID, err := api.HashObject(ctx, bytes.NewReader([]byte("hi\n")))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", blobOID, "dir/file.txt"); err != nil {
		t.Fatal(err)
	}
	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := api.CommitTree(ctx, treeOID, nil, "seed", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	gotOID, gotTyp, err := api.ResolvePathObject(ctx, commitOID, "dir/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if gotTyp != ObjectTypeBlob {
		t.Fatalf("expected type blob, got %q", gotTyp)
	}
	if gotOID != blobOID {
		t.Fatalf("expected oid %q, got %q", blobOID, gotOID)
	}

	_, gotTyp, err = api.ResolvePathObject(ctx, commitOID, "dir")
	if err != nil {
		t.Fatal(err)
	}
	if gotTyp != ObjectTypeTree {
		t.Fatalf("expected type tree, got %q", gotTyp)
	}
}

func TestGitAPIImpl_ListTree_NonRecursive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, _, api := newTestRepo(t, ctx)

	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}

	oidA, err := api.HashObject(ctx, bytes.NewReader([]byte("a\n")))
	if err != nil {
		t.Fatal(err)
	}
	oidB, err := api.HashObject(ctx, bytes.NewReader([]byte("b\n")))
	if err != nil {
		t.Fatal(err)
	}
	oidX, err := api.HashObject(ctx, bytes.NewReader([]byte("x\n")))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oidA, "dir/a.txt"); err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oidB, "dir/b.txt"); err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oidX, "dir/sub/x.txt"); err != nil {
		t.Fatal(err)
	}

	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := api.CommitTree(ctx, treeOID, nil, "seed", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	entries, err := api.ListTree(ctx, commitOID, "dir")
	if err != nil {
		t.Fatal(err)
	}
	// Expect: a.txt (blob), b.txt (blob), sub (tree)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d: %+v", len(entries), entries)
	}

	var gotA, gotB, gotSub bool
	for _, e := range entries {
		switch e.Name {
		case "a.txt":
			gotA = true
			if e.Type != ObjectTypeBlob || e.OID != oidA {
				t.Fatalf("unexpected a.txt entry: %+v", e)
			}
		case "b.txt":
			gotB = true
			if e.Type != ObjectTypeBlob || e.OID != oidB {
				t.Fatalf("unexpected b.txt entry: %+v", e)
			}
		case "sub":
			gotSub = true
			if e.Type != ObjectTypeTree || e.OID == "" {
				t.Fatalf("unexpected sub entry: %+v", e)
			}
		default:
			t.Fatalf("unexpected entry: %+v", e)
		}
	}
	if !gotA || !gotB || !gotSub {
		t.Fatalf("missing expected entries: gotA=%v gotB=%v gotSub=%v", gotA, gotB, gotSub)
	}
}

func TestGitAPIImpl_RemoveIndexPaths_RemovesFromIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, _, api := newTestRepo(t, ctx)

	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}

	oidA, err := api.HashObject(ctx, bytes.NewReader([]byte("a\n")))
	if err != nil {
		t.Fatal(err)
	}
	oidB, err := api.HashObject(ctx, bytes.NewReader([]byte("b\n")))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oidA, "a.txt"); err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oidB, "b.txt"); err != nil {
		t.Fatal(err)
	}

	if err := api.RemoveIndexPaths(ctx, indexFile, []string{"a.txt"}); err != nil {
		t.Fatal(err)
	}

	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := api.CommitTree(ctx, treeOID, nil, "seed", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	// a.txt removed, b.txt still present
	_, err = api.ResolvePathBlob(ctx, commitOID, "a.txt")
	if err == nil {
		t.Fatalf("expected a.txt missing")
	}
	var pnf *PathNotFoundError
	if !errors.As(err, &pnf) {
		t.Fatalf("expected PathNotFoundError, got %T: %v", err, err)
	}

	gotB, err := api.ResolvePathBlob(ctx, commitOID, "b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if gotB != oidB {
		t.Fatalf("expected b.txt oid %q, got %q", oidB, gotB)
	}
}

func TestGitAPIImpl_ReadTree_PreservesExistingPaths(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, r, api := newTestRepo(t, ctx)

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

func TestGitAPIImpl_UpdateRef_And_CAS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, _, api := newTestRepo(t, ctx)

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

func TestGitAPIImpl_FetchRef_ForcedUpdatesTrackingRef(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	remoteRepo, _, remoteAPI := newTestRepo(t, ctx)

	// Create two commits on the same tree in the remote.
	indexFile := tempIndexFile(t)
	if err := remoteAPI.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}
	treeOID, err := remoteAPI.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	c1, err := remoteAPI.CommitTree(ctx, treeOID, nil, "c1", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := remoteAPI.CommitTree(ctx, treeOID, nil, "c2", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	if c1 == c2 {
		c2, err = remoteAPI.CommitTree(ctx, treeOID, nil, "c2b", testAuthor())
		if err != nil {
			t.Fatal(err)
		}
		if c1 == c2 {
			t.Fatalf("expected distinct commit oids")
		}
	}

	remoteDataRef := "refs/dolt/data"
	if err := remoteAPI.UpdateRef(ctx, remoteDataRef, c2, "seed remote"); err != nil {
		t.Fatal(err)
	}

	_, localRunner, localAPI := newTestRepo(t, ctx)
	_, err = localRunner.Run(ctx, RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	if err != nil {
		t.Fatal(err)
	}

	dstRef := "refs/dolt/remotes/origin/data"
	if err := localAPI.FetchRef(ctx, "origin", remoteDataRef, dstRef); err != nil {
		t.Fatal(err)
	}
	got, err := localAPI.ResolveRefCommit(ctx, dstRef)
	if err != nil {
		t.Fatal(err)
	}
	if got != c2 {
		t.Fatalf("tracking ref mismatch: got %q, want %q", got, c2)
	}

	// Rewind the remote ref to c1 and ensure a subsequent fetch forces the tracking ref backwards.
	if err := remoteAPI.UpdateRef(ctx, remoteDataRef, c1, "rewind remote"); err != nil {
		t.Fatal(err)
	}
	if err := localAPI.FetchRef(ctx, "origin", remoteDataRef, dstRef); err != nil {
		t.Fatal(err)
	}
	got, err = localAPI.ResolveRefCommit(ctx, dstRef)
	if err != nil {
		t.Fatal(err)
	}
	if got != c1 {
		t.Fatalf("tracking ref mismatch after rewind: got %q, want %q", got, c1)
	}
}

func TestGitAPIImpl_PushRefWithLease_SucceedsThenRejectsStaleLease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	remoteRepo, _, remoteAPI := newTestRepo(t, ctx)

	// Seed remote ref with r1, then later advance to r2.
	indexFile := tempIndexFile(t)
	if err := remoteAPI.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}
	treeOID, err := remoteAPI.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := remoteAPI.CommitTree(ctx, treeOID, nil, "r1", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	r2, err := remoteAPI.CommitTree(ctx, treeOID, nil, "r2", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	if r1 == r2 {
		r2, err = remoteAPI.CommitTree(ctx, treeOID, nil, "r2b", testAuthor())
		if err != nil {
			t.Fatal(err)
		}
		if r1 == r2 {
			t.Fatalf("expected distinct commit oids")
		}
	}

	remoteDataRef := "refs/dolt/data"
	if err := remoteAPI.UpdateRef(ctx, remoteDataRef, r1, "seed remote"); err != nil {
		t.Fatal(err)
	}

	_, localRunner, localAPI := newTestRepo(t, ctx)
	_, err = localRunner.Run(ctx, RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	if err != nil {
		t.Fatal(err)
	}

	// Create a local commit l1 and set local refs/dolt/data to it (src ref for push).
	localIndex := tempIndexFile(t)
	if err := localAPI.ReadTreeEmpty(ctx, localIndex); err != nil {
		t.Fatal(err)
	}
	localTree, err := localAPI.WriteTree(ctx, localIndex)
	if err != nil {
		t.Fatal(err)
	}
	l1, err := localAPI.CommitTree(ctx, localTree, nil, "l1", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	if err := localAPI.UpdateRef(ctx, remoteDataRef, l1, "set local src"); err != nil {
		t.Fatal(err)
	}

	// Lease matches remote (r1) -> push should succeed and overwrite remoteDataRef to l1.
	if err := localAPI.PushRefWithLease(ctx, "origin", remoteDataRef, remoteDataRef, r1); err != nil {
		t.Fatal(err)
	}
	got, err := remoteAPI.ResolveRefCommit(ctx, remoteDataRef)
	if err != nil {
		t.Fatal(err)
	}
	if got != l1 {
		t.Fatalf("remote ref mismatch after push: got %q, want %q", got, l1)
	}

	// Advance remote to r2, then attempt a stale-lease push expecting r1 -> should fail and not clobber r2.
	if err := remoteAPI.UpdateRef(ctx, remoteDataRef, r2, "advance remote"); err != nil {
		t.Fatal(err)
	}
	err = localAPI.PushRefWithLease(ctx, "origin", remoteDataRef, remoteDataRef, r1)
	if err == nil {
		t.Fatalf("expected stale lease push to fail")
	}
	got, err = remoteAPI.ResolveRefCommit(ctx, remoteDataRef)
	if err != nil {
		t.Fatal(err)
	}
	if got != r2 {
		t.Fatalf("remote ref changed unexpectedly on stale lease: got %q, want %q", got, r2)
	}
}

func TestGitAPIImpl_MergeBase_OkAndNoMergeBase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, _, api := newTestRepo(t, ctx)

	// Base commit (empty tree).
	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}
	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	base, err := api.CommitTree(ctx, treeOID, nil, "base", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	// Two children of base.
	p := base
	a, err := api.CommitTree(ctx, treeOID, &p, "a", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	p = base
	b, err := api.CommitTree(ctx, treeOID, &p, "b", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	mb, ok, err := api.MergeBase(ctx, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("expected merge base")
	}
	if mb != base {
		t.Fatalf("merge base mismatch: got %q, want %q", mb, base)
	}

	// No merge base: two unrelated root commits in the same repo.
	c1, err := api.CommitTree(ctx, treeOID, nil, "root1", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := api.CommitTree(ctx, treeOID, nil, "root2", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err = api.MergeBase(ctx, c1, c2)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected no merge base")
	}
}

func TestGitAPIImpl_ListTreeRecursive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, _, api := newTestRepo(t, ctx)

	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}

	oidA, err := api.HashObject(ctx, bytes.NewReader([]byte("a\n")))
	if err != nil {
		t.Fatal(err)
	}
	oidX, err := api.HashObject(ctx, bytes.NewReader([]byte("x\n")))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oidA, "dir/a.txt"); err != nil {
		t.Fatal(err)
	}
	if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", oidX, "dir/sub/x.txt"); err != nil {
		t.Fatal(err)
	}
	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := api.CommitTree(ctx, treeOID, nil, "seed", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	entries, err := api.ListTreeRecursive(ctx, commitOID, "dir")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]TreeEntry{}
	for _, e := range entries {
		got[e.Name] = e
	}
	if e, ok := got["a.txt"]; !ok || e.Type != ObjectTypeBlob || e.OID != oidA {
		t.Fatalf("missing or unexpected a.txt: ok=%v entry=%+v", ok, e)
	}
	if e, ok := got["sub"]; !ok || e.Type != ObjectTypeTree || e.OID == "" {
		t.Fatalf("missing or unexpected sub tree: ok=%v entry=%+v", ok, e)
	}
	if e, ok := got["sub/x.txt"]; !ok || e.Type != ObjectTypeBlob || e.OID != oidX {
		t.Fatalf("missing or unexpected sub/x.txt: ok=%v entry=%+v", ok, e)
	}
}

func TestGitAPIImpl_CommitTreeWithParents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, r, api := newTestRepo(t, ctx)

	indexFile := tempIndexFile(t)
	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		t.Fatal(err)
	}
	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		t.Fatal(err)
	}
	base, err := api.CommitTree(ctx, treeOID, nil, "base", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	p := base
	a, err := api.CommitTree(ctx, treeOID, &p, "a", testAuthor())
	if err != nil {
		t.Fatal(err)
	}
	p = base
	b, err := api.CommitTree(ctx, treeOID, &p, "b", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	merge, err := api.CommitTreeWithParents(ctx, treeOID, []OID{a, b}, "merge", testAuthor())
	if err != nil {
		t.Fatal(err)
	}

	out, err := r.Run(ctx, RunOptions{}, "rev-parse", merge.String()+"^1")
	if err != nil {
		t.Fatal(err)
	}
	if gotP1 := string(bytes.TrimSpace(out)); gotP1 != a.String() {
		t.Fatalf("parent1 mismatch: got %q, want %q", gotP1, a.String())
	}
	out, err = r.Run(ctx, RunOptions{}, "rev-parse", merge.String()+"^2")
	if err != nil {
		t.Fatal(err)
	}
	if gotP2 := string(bytes.TrimSpace(out)); gotP2 != b.String() {
		t.Fatalf("parent2 mismatch: got %q, want %q", gotP2, b.String())
	}
}
