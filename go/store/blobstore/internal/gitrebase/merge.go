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

// Package gitrebase contains higher-level orchestration for Git-backed storage sync.
//
// This is intentionally separate from the lower-level plumbing in internal/git:
// internal/git provides primitives (rev-parse, fetch, update-ref, etc.), while this
// package encodes storage-level policies (fast-forward vs merge, conflict policy, retries).
package gitrebase

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

type MergeResult int

const (
	MergeUpToDate MergeResult = iota
	MergeFastForward
	MergeMerged
)

// MergeRemoteTrackingIntoLocalRef merges |remoteTrackingRef| into |localRef|, updating |localRef|.
//
// Conflict policy: if the same path is changed differently on both sides (relative to the merge-base),
// this returns *git.MergeConflictError and leaves |localRef| unchanged.
func MergeRemoteTrackingIntoLocalRef(ctx context.Context, api git.GitAPI, localRef, remoteTrackingRef string, author *git.Identity) (newHead git.OID, result MergeResult, err error) {
	remote, remoteOK, err := api.TryResolveRefCommit(ctx, remoteTrackingRef)
	if err != nil {
		return "", 0, err
	}
	if !remoteOK {
		// Nothing to merge.
		local, ok, err := api.TryResolveRefCommit(ctx, localRef)
		if err != nil {
			return "", 0, err
		}
		if !ok {
			return "", MergeUpToDate, nil
		}
		return local, MergeUpToDate, nil
	}

	const maxRetries = 31
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 5 * time.Millisecond
	bo.Multiplier = 2
	bo.MaxInterval = 320 * time.Millisecond
	bo.RandomizationFactor = 0
	bo.Reset()
	policy := backoff.WithContext(backoff.WithMaxRetries(bo, maxRetries), ctx)

	var head git.OID
	var res MergeResult
	op := func() error {
		local, localOK, err := api.TryResolveRefCommit(ctx, localRef)
		if err != nil {
			return backoff.Permanent(err)
		}

		// If local is missing, just create it at remote's head (create-only CAS).
		if !localOK {
			msg := fmt.Sprintf("gitblobstore: storage rebase (ff) %s <- %s", localRef, remoteTrackingRef)
			const zeroOID = git.OID("0000000000000000000000000000000000000000")
			if err := api.UpdateRefCAS(ctx, localRef, remote, zeroOID, msg); err != nil {
				return err
			}
			head = remote
			res = MergeFastForward
			return nil
		}

		if local == remote {
			head = local
			res = MergeUpToDate
			return nil
		}

		base, ok, err := api.MergeBase(ctx, local, remote)
		if err != nil {
			return backoff.Permanent(err)
		}
		// Determine fast-forward / up-to-date using merge-base.
		if ok && base == local {
			msg := fmt.Sprintf("gitblobstore: storage rebase (ff) %s <- %s", localRef, remoteTrackingRef)
			if err := api.UpdateRefCAS(ctx, localRef, remote, local, msg); err != nil {
				if refAdvanced(ctx, api, localRef, local) {
					return err
				}
				return backoff.Permanent(err)
			}
			head = remote
			res = MergeFastForward
			return nil
		}
		if ok && base == remote {
			head = local
			res = MergeUpToDate
			return nil
		}

		mergedCommit, err := mergeCommitsNoUpdate(ctx, api, base, ok, local, remote, author)
		if err != nil {
			return backoff.Permanent(err)
		}
		msg := fmt.Sprintf("gitblobstore: storage rebase (merge) %s <- %s", localRef, remoteTrackingRef)
		if err := api.UpdateRefCAS(ctx, localRef, mergedCommit, local, msg); err != nil {
			if refAdvanced(ctx, api, localRef, local) {
				return err
			}
			return backoff.Permanent(err)
		}
		head = mergedCommit
		res = MergeMerged
		return nil
	}

	if err := backoff.Retry(op, policy); err != nil {
		if ctx.Err() != nil {
			return "", 0, ctx.Err()
		}
		return "", 0, err
	}
	return head, res, nil
}

func refAdvanced(ctx context.Context, api git.GitAPI, ref string, old git.OID) bool {
	if ctx.Err() != nil {
		return false
	}
	cur, ok, err := api.TryResolveRefCommit(ctx, ref)
	return err == nil && ok && cur != old
}

func mergeCommitsNoUpdate(ctx context.Context, api git.GitAPI, base git.OID, hasBase bool, ours git.OID, theirs git.OID, author *git.Identity) (git.OID, error) {
	baseEntries := map[string]git.TreeEntry{}
	if hasBase {
		ents, err := api.ListTreeRecursive(ctx, base)
		if err != nil {
			return "", err
		}
		baseEntries = entriesToMap(ents)
	}

	oursEnts, err := api.ListTreeRecursive(ctx, ours)
	if err != nil {
		return "", err
	}
	theirsEnts, err := api.ListTreeRecursive(ctx, theirs)
	if err != nil {
		return "", err
	}
	oursEntries := entriesToMap(oursEnts)
	theirsEntries := entriesToMap(theirsEnts)

	paths := unionPaths(baseEntries, oursEntries, theirsEntries)
	sort.Strings(paths)

	merged := make([]git.TreeEntry, 0, len(paths))
	var conflicts []string
	for _, p := range paths {
		b, bok := baseEntries[p]
		o, ook := oursEntries[p]
		t, tok := theirsEntries[p]

		// Only support blob entries (expected for GitBlobstore keyspace).
		for _, e := range []struct {
			ok  bool
			ent git.TreeEntry
		}{{bok, b}, {ook, o}, {tok, t}} {
			if e.ok && e.ent.Type != "blob" {
				return "", fmt.Errorf("merge unsupported non-blob entry %q type=%q", e.ent.Path, e.ent.Type)
			}
		}

		boid := entryOID(bok, b)
		ooid := entryOID(ook, o)
		toid := entryOID(tok, t)

		oursChanged := ooid != boid
		theirsChanged := toid != boid

		switch {
		case !oursChanged && !theirsChanged:
			if bok {
				merged = append(merged, b)
			}
		case oursChanged && !theirsChanged:
			if ook {
				merged = append(merged, o)
			}
		case !oursChanged && theirsChanged:
			if tok {
				merged = append(merged, t)
			}
		default: // both changed
			if ooid == toid {
				if ook {
					merged = append(merged, o)
				}
			} else {
				conflicts = append(conflicts, p)
			}
		}
	}
	if len(conflicts) > 0 {
		return "", &git.MergeConflictError{Paths: conflicts}
	}

	indexFile, cleanup, err := newTempIndexFile()
	if err != nil {
		return "", err
	}
	defer cleanup()

	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		return "", err
	}
	for _, e := range merged {
		if e.Path == "" || e.OID == "" {
			continue
		}
		if err := api.UpdateIndexCacheInfo(ctx, indexFile, e.Mode, e.OID, e.Path); err != nil {
			return "", err
		}
	}
	treeOID, err := api.WriteTree(ctx, indexFile)
	if err != nil {
		return "", err
	}

	msg := fmt.Sprintf("gitblobstore: storage rebase merge %s and %s", ours, theirs)
	commitOID, err := api.CommitTreeWithParents(ctx, treeOID, []git.OID{ours, theirs}, msg, author)
	if err != nil && author == nil && isMissingGitIdentityErr(err) {
		commitOID, err = api.CommitTreeWithParents(ctx, treeOID, []git.OID{ours, theirs}, msg, defaultGitBlobstoreIdentity())
	}
	if err != nil {
		return "", err
	}
	return commitOID, nil
}

func newTempIndexFile() (path string, cleanup func(), err error) {
	f, err := os.CreateTemp("", "dolt-gitrebase-index-")
	if err != nil {
		return "", nil, err
	}
	path = f.Name()
	_ = f.Close()
	cleanup = func() {
		_ = os.Remove(path)
		_ = os.Remove(path + ".lock")
	}
	return path, cleanup, nil
}

func defaultGitBlobstoreIdentity() *git.Identity {
	return &git.Identity{Name: "dolt gitblobstore", Email: "gitblobstore@dolt.invalid"}
}

func isMissingGitIdentityErr(err error) bool {
	var ce *git.CmdError
	if !errors.As(err, &ce) {
		return false
	}
	msg := strings.ToLower(string(ce.Output))
	return strings.Contains(msg, "author identity unknown") ||
		strings.Contains(msg, "unable to auto-detect email address") ||
		strings.Contains(msg, "empty ident name")
}

func entriesToMap(ents []git.TreeEntry) map[string]git.TreeEntry {
	m := make(map[string]git.TreeEntry, len(ents))
	for _, e := range ents {
		m[e.Path] = e
	}
	return m
}

func unionPaths(ms ...map[string]git.TreeEntry) []string {
	seen := map[string]struct{}{}
	for _, m := range ms {
		for p := range m {
			seen[p] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	return out
}

func entryOID(ok bool, e git.TreeEntry) git.OID {
	if !ok {
		return ""
	}
	return e.OID
}
