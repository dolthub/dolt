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
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

// TestGitBlobstore_Prune_RemovesUnreferencedEntries writes several table file
// entries to the remote, then writes a manifest that only references a subset.
// The resulting git tree should only contain the referenced entries plus the
// manifest itself.
func TestGitBlobstore_Prune_RemovesUnreferencedEntries(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)

	// Seed the remote with a tree containing three "table files" and a manifest
	// that references all three.
	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": []byte("5:__DOLT__:lock:root:gc:tableA:10:tableB:20:tableC:30"),
		"tableA":   []byte("data-a"),
		"tableB":   []byte("data-b"),
		"tableC":   []byte("data-c"),
	}, "seed with 3 tables")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
	})
	require.NoError(t, err)

	// Get current manifest version for CheckAndPut.
	_, ver, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)

	// Write a new manifest that only references tableA — tableB and tableC should be pruned.
	newManifest := []byte("5:__DOLT__:lock2:root2:gc2:tableA:10")
	_, err = bs.CheckAndPut(ctx, ver, "manifest", int64(len(newManifest)), bytes.NewReader(newManifest))
	require.NoError(t, err)

	// Inspect the remote tree — it should only contain "manifest" and "tableA".
	remoteHead, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	entries, err := remoteAPI.ListTreeRecursive(ctx, remoteHead)
	require.NoError(t, err)

	entryNames := make(map[string]bool)
	for _, e := range entries {
		entryNames[e.Name] = true
	}

	require.True(t, entryNames["manifest"], "manifest should be in tree")
	require.True(t, entryNames["tableA"], "tableA should be in tree")
	require.False(t, entryNames["tableB"], "tableB should have been pruned")
	require.False(t, entryNames["tableC"], "tableC should have been pruned")
}

// TestGitBlobstore_Prune_CreatesOrphanCommit verifies that when entries are
// pruned, the resulting commit has no parent (orphan), so old bloated trees
// become unreachable.
func TestGitBlobstore_Prune_CreatesOrphanCommit(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)

	// Seed with dead entries.
	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": []byte("5:__DOLT__:lock:root:gc:tableA:10:dead1:20"),
		"tableA":   []byte("data-a"),
		"dead1":    []byte("dead-data"),
	}, "seed")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
	})
	require.NoError(t, err)

	_, ver, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)

	// Write manifest that drops dead1 — this should trigger pruning + orphan commit.
	newManifest := []byte("5:__DOLT__:lock2:root2:gc2:tableA:10")
	_, err = bs.CheckAndPut(ctx, ver, "manifest", int64(len(newManifest)), bytes.NewReader(newManifest))
	require.NoError(t, err)

	remoteHead, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)

	// cat-file -p the commit — orphan commits have no "parent" line.
	out, err := remoteRunner.Run(ctx, git.RunOptions{}, "cat-file", "-p", remoteHead.String())
	require.NoError(t, err)
	require.NotContains(t, string(out), "\nparent ", "commit should be an orphan after pruning")
}

// TestGitBlobstore_Prune_NoPruneWhenAllReferenced verifies that when the
// manifest references all existing entries, no pruning occurs and the commit
// retains its parent.
func TestGitBlobstore_Prune_NoPruneWhenAllReferenced(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)

	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": []byte("5:__DOLT__:lock:root:gc:tableA:10"),
		"tableA":   []byte("data-a"),
	}, "seed")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
	})
	require.NoError(t, err)

	_, ver, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)

	// Update manifest but keep referencing the same table — no pruning should happen.
	newManifest := []byte("5:__DOLT__:lock2:root2:gc2:tableA:10")
	_, err = bs.CheckAndPut(ctx, ver, "manifest", int64(len(newManifest)), bytes.NewReader(newManifest))
	require.NoError(t, err)

	remoteHead, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)

	// Should have a parent since nothing was pruned.
	out, err := remoteRunner.Run(ctx, git.RunOptions{}, "cat-file", "-p", remoteHead.String())
	require.NoError(t, err)
	require.Contains(t, string(out), "\nparent ", "commit should have a parent when nothing was pruned")
}

// TestGitBlobstore_Prune_ChunkedAndSuffixedEntries verifies that pruning
// correctly handles chunked paths (key/NNNN) and suffixed entries (.records,
// .tail, .darc).
func TestGitBlobstore_Prune_ChunkedAndSuffixedEntries(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()

	remoteRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/remote.git")
	require.NoError(t, err)
	remoteRunner, err := git.NewRunner(remoteRepo.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(remoteRunner)

	// Seed tree with various entry types: chunked parts, .records, .tail, .darc
	_, err = remoteRepo.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest":         []byte("5:__DOLT__:lock:root:gc:keepme:10"),
		"keepme":           []byte("kept"),
		"keepme.records":   []byte("kept-records"),
		"keepme.tail":      []byte("kept-tail"),
		"dead/0001":        []byte("dead-chunk-1"),
		"dead/0002":        []byte("dead-chunk-2"),
		"dead2.darc":       []byte("dead-archive"),
		"dead3.records":    []byte("dead-records"),
		"dead3.tail":       []byte("dead-tail"),
	}, "seed with mixed entries")
	require.NoError(t, err)

	localRepo, err := gitrepo.InitBare(ctx, t.TempDir()+"/local.git")
	require.NoError(t, err)
	localRunner, err := git.NewRunner(localRepo.GitDir)
	require.NoError(t, err)
	_, err = localRunner.Run(ctx, git.RunOptions{}, "remote", "add", "origin", remoteRepo.GitDir)
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(localRepo.GitDir, DoltDataRef, GitBlobstoreOptions{
		RemoteName: "origin",
		Identity:   testIdentity(),
	})
	require.NoError(t, err)

	_, ver, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)

	// Manifest only references "keepme".
	newManifest := []byte("5:__DOLT__:lock2:root2:gc2:keepme:10")
	_, err = bs.CheckAndPut(ctx, ver, "manifest", int64(len(newManifest)), bytes.NewReader(newManifest))
	require.NoError(t, err)

	remoteHead, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	entries, err := remoteAPI.ListTreeRecursive(ctx, remoteHead)
	require.NoError(t, err)

	entryNames := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Type == git.ObjectTypeBlob {
			entryNames = append(entryNames, e.Name)
		}
	}

	// Only manifest, keepme, and keepme's related entries should remain.
	expected := map[string]bool{
		"manifest":       true,
		"keepme":         true,
		"keepme.records": true,
		"keepme.tail":    true,
	}
	for _, name := range entryNames {
		require.True(t, expected[name], "unexpected entry in tree: %s", name)
	}
	for name := range expected {
		found := false
		for _, n := range entryNames {
			if n == name {
				found = true
				break
			}
		}
		require.True(t, found, "expected entry missing from tree: %s", name)
	}

	// Verify orphan commit since entries were pruned.
	out, err := remoteRunner.Run(ctx, git.RunOptions{}, "cat-file", "-p", remoteHead.String())
	require.NoError(t, err)
	require.NotContains(t, string(out), "\nparent ")

}
