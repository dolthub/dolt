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
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

type unitKind uint8

const (
	unitKindMissing unitKind = iota
	unitKindBlob
	unitKindChunkedTree
)

type unit struct {
	kind unitKind
	oid  git.OID // blob oid for blob, tree oid for chunked tree
	// parts contains file-path -> blob oid for a chunked tree's parts, using full paths "k/0001".
	parts map[string]git.OID
}

func (u unit) equal(v unit) bool {
	if u.kind != v.kind || u.oid != v.oid {
		return false
	}
	// oid is sufficient identity for chunked trees and blobs.
	return true
}

type treeSnapshot struct {
	// files contains blob entries not under a chunked-tree root.
	files map[string]git.OID
	// chunked contains chunked-tree roots (tree entries) and their part blobs.
	chunked map[string]unit
}

func snapshotFromRecursive(entries []git.TreeEntry) (treeSnapshot, error) {
	trees := make(map[string]git.TreeEntry)
	blobs := make(map[string]git.OID)
	for _, e := range entries {
		switch e.Type {
		case git.ObjectTypeTree:
			trees[e.Name] = e
		case git.ObjectTypeBlob:
			blobs[e.Name] = e.OID
		default:
			// ignore; gitrebase only merges trees of blob/tree entries
		}
	}

	// Identify chunked roots among tree entries.
	chunked := make(map[string]unit)
	for treePath, te := range trees {
		parts, ok := detectChunkedParts(treePath, entries)
		if !ok {
			continue
		}
		u := unit{kind: unitKindChunkedTree, oid: te.OID, parts: parts}
		chunked[treePath] = u
	}

	files := make(map[string]git.OID, len(blobs))
	for p, oid := range blobs {
		if isUnderAnyChunkedRoot(p, chunked) {
			continue
		}
		files[p] = oid
	}
	return treeSnapshot{files: files, chunked: chunked}, nil
}

func isUnderAnyChunkedRoot(path string, chunked map[string]unit) bool {
	for root := range chunked {
		if root == "" {
			continue
		}
		if strings.HasPrefix(path, root+"/") {
			return true
		}
	}
	return false
}

func detectChunkedParts(treePath string, entries []git.TreeEntry) (parts map[string]git.OID, ok bool) {
	// Chunked tree heuristic:
	// - it is a tree entry with one or more immediate children
	// - all immediate children names are 4-digit strings ("0001", ...)
	// - all immediate children are blobs
	// - there are no deeper descendants (since children are blobs)
	prefix := treePath
	if prefix != "" {
		prefix += "/"
	}
	imm := make(map[string]git.OID)
	for _, e := range entries {
		if e.Type != git.ObjectTypeBlob {
			continue
		}
		if !strings.HasPrefix(e.Name, prefix) {
			continue
		}
		rest := strings.TrimPrefix(e.Name, prefix)
		if rest == "" || strings.Contains(rest, "/") {
			continue
		}
		imm[rest] = e.OID
	}
	if len(imm) == 0 {
		return nil, false
	}
	for name := range imm {
		if len(name) != 4 {
			return nil, false
		}
		for i := 0; i < 4; i++ {
			c := name[i]
			if c < '0' || c > '9' {
				return nil, false
			}
		}
	}
	parts = make(map[string]git.OID, len(imm))
	for partName, oid := range imm {
		full := treePath
		if full != "" {
			full += "/"
		}
		full += partName
		parts[full] = oid
	}
	return parts, true
}

func unitsFromSnapshot(s treeSnapshot) map[string]unit {
	out := make(map[string]unit, len(s.files)+len(s.chunked))
	for p, oid := range s.files {
		out[p] = unit{kind: unitKindBlob, oid: oid}
	}
	for p, u := range s.chunked {
		out[p] = u
	}
	return out
}

// MergeRemoteTrackingIntoLocalRef merges |remoteTrackingRef| into |localRef| and updates |localRef| using CAS.
//
// Behavior:
// - If |localRef| does not exist, it is created to point to |remoteTrackingRef| (create-only CAS).
// - If |localRef| is an ancestor of |remoteTrackingRef|, |localRef| is fast-forwarded to |remoteTrackingRef|.
// - If |remoteTrackingRef| is an ancestor of |localRef|, no update is performed.
// - Otherwise, a 3-way merge is performed at the GitBlobstore key/path granularity:
//   - blob entries merge by path
//   - chunked trees (trees whose children are 4-digit part blobs) merge atomically as a unit
//
// - On conflict, returns *MergeConflictError and does not update |localRef|.
func MergeRemoteTrackingIntoLocalRef(ctx context.Context, api git.GitAPI, localRef string, remoteTrackingRef string, msg string, author *git.Identity) (newHead git.OID, updated bool, err error) {
	return MergeRemoteTrackingIntoLocalRefWithOptions(ctx, api, localRef, remoteTrackingRef, MergeOptions{
		Message:    msg,
		Author:     author,
		OnConflict: nil,
	})
}

// MergeOptions controls merge behavior and conflict handling.
type MergeOptions struct {
	Message string
	Author  *git.Identity
	// OnConflict, if provided, is invoked when a merge conflict is detected.
	// If handled=true is returned, the merge helper returns (newHead, updated, nil).
	OnConflict ConflictHook
}

// ConflictHook allows callers (e.g. GitBlobstore) to implement merge conflict policy.
// This is intended for the "remote truth + replay" flow: callers can decide how to resolve conflicts and
// update |localRef| accordingly.
type ConflictHook func(ctx context.Context, api git.GitAPI, localRef string, remoteTrackingRef string, base git.OID, localHead git.OID, remoteHead git.OID, conflict *git.MergeConflictError, msg string, author *git.Identity) (newHead git.OID, updated bool, handled bool, err error)

// ConflictRemoteWins is a simple conflict policy that treats the remote as source-of-truth by moving
// |localRef| to |remoteHead| using CAS.
func ConflictRemoteWins(ctx context.Context, api git.GitAPI, localRef string, remoteTrackingRef string, base git.OID, localHead git.OID, remoteHead git.OID, conflict *git.MergeConflictError, msg string, author *git.Identity) (git.OID, bool, bool, error) {
	if err := api.UpdateRefCAS(ctx, localRef, remoteHead, localHead, msg); err != nil {
		return "", false, true, err
	}
	return remoteHead, true, true, nil
}

func MergeRemoteTrackingIntoLocalRefWithOptions(ctx context.Context, api git.GitAPI, localRef string, remoteTrackingRef string, opts MergeOptions) (newHead git.OID, updated bool, err error) {
	if localRef == "" {
		return "", false, fmt.Errorf("gitrebase: localRef is required")
	}
	if remoteTrackingRef == "" {
		return "", false, fmt.Errorf("gitrebase: remoteTrackingRef is required")
	}
	msg := opts.Message
	author := opts.Author

	remoteHead, okRemote, err := api.TryResolveRefCommit(ctx, remoteTrackingRef)
	if err != nil {
		return "", false, err
	}
	if !okRemote {
		// Nothing to merge.
		return "", false, &git.RefNotFoundError{Ref: remoteTrackingRef}
	}

	localHead, okLocal, err := api.TryResolveRefCommit(ctx, localRef)
	if err != nil {
		return "", false, err
	}
	if !okLocal {
		// Create-only CAS to remote head.
		const zeroOID = git.OID("0000000000000000000000000000000000000000")
		if err := api.UpdateRefCAS(ctx, localRef, remoteHead, zeroOID, msg); err != nil {
			return "", false, err
		}
		return remoteHead, true, nil
	}
	if localHead == remoteHead {
		return localHead, false, nil
	}

	base, ok, err := api.MergeBase(ctx, localHead, remoteHead)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return handleConflict(ctx, api, localRef, remoteTrackingRef, "", localHead, remoteHead, &git.MergeConflictError{Conflicts: []string{"(no merge base)"}}, msg, author, opts.OnConflict)
	}

	if base == localHead {
		// Fast-forward local -> remote.
		if err := api.UpdateRefCAS(ctx, localRef, remoteHead, localHead, msg); err != nil {
			return "", false, err
		}
		return remoteHead, true, nil
	}
	if base == remoteHead {
		// Remote is behind local; nothing to do for a remote->local merge.
		return localHead, false, nil
	}

	baseEntries, err := api.ListTreeRecursive(ctx, base, "")
	if err != nil {
		return "", false, err
	}
	localEntries, err := api.ListTreeRecursive(ctx, localHead, "")
	if err != nil {
		return "", false, err
	}
	remoteEntries, err := api.ListTreeRecursive(ctx, remoteHead, "")
	if err != nil {
		return "", false, err
	}

	baseSnap, err := snapshotFromRecursive(baseEntries)
	if err != nil {
		return "", false, err
	}
	localSnap, err := snapshotFromRecursive(localEntries)
	if err != nil {
		return "", false, err
	}
	remoteSnap, err := snapshotFromRecursive(remoteEntries)
	if err != nil {
		return "", false, err
	}

	baseUnits := unitsFromSnapshot(baseSnap)
	localUnits := unitsFromSnapshot(localSnap)
	remoteUnits := unitsFromSnapshot(remoteSnap)

	mergedUnits, conflicts := mergeUnits(baseUnits, localUnits, remoteUnits)
	if conflicts != nil {
		conflicts.Conflicts = sortAndUniq(conflicts.Conflicts)
		return handleConflict(ctx, api, localRef, remoteTrackingRef, base, localHead, remoteHead, conflicts, msg, author, opts.OnConflict)
	}

	mergedTree, err := writeMergedTree(ctx, api, mergedUnits, baseSnap, localSnap, remoteSnap)
	if err != nil {
		return "", false, err
	}

	mergeCommit, err := api.CommitTreeWithParents(ctx, mergedTree, []git.OID{localHead, remoteHead}, msg, author)
	if err != nil && author == nil && isMissingGitIdentityErr(err) {
		mergeCommit, err = api.CommitTreeWithParents(ctx, mergedTree, []git.OID{localHead, remoteHead}, msg, defaultIdentity())
	}
	if err != nil {
		return "", false, err
	}

	if err := api.UpdateRefCAS(ctx, localRef, mergeCommit, localHead, msg); err != nil {
		return "", false, err
	}
	return mergeCommit, true, nil
}

func defaultIdentity() *git.Identity {
	return &git.Identity{Name: "dolt gitrebase", Email: "gitrebase@dolt.invalid"}
}

func mergeUnits(base map[string]unit, local map[string]unit, remote map[string]unit) (map[string]unit, *git.MergeConflictError) {
	out := make(map[string]unit)
	conflicts := &git.MergeConflictError{}

	all := make(map[string]struct{}, len(base)+len(local)+len(remote))
	for k := range base {
		all[k] = struct{}{}
	}
	for k := range local {
		all[k] = struct{}{}
	}
	for k := range remote {
		all[k] = struct{}{}
	}

	for k := range all {
		b, bok := base[k]
		l, lok := local[k]
		r, rok := remote[k]
		if !bok {
			b = unit{kind: unitKindMissing}
		}
		if !lok {
			l = unit{kind: unitKindMissing}
		}
		if !rok {
			r = unit{kind: unitKindMissing}
		}

		if l.equal(r) {
			if l.kind != unitKindMissing {
				out[k] = l
			}
			continue
		}
		if l.equal(b) {
			if r.kind != unitKindMissing {
				out[k] = r
			}
			continue
		}
		if r.equal(b) {
			if l.kind != unitKindMissing {
				out[k] = l
			}
			continue
		}

		// Both changed differently.
		conflicts.Conflicts = append(conflicts.Conflicts, k)
	}

	if len(conflicts.Conflicts) > 0 {
		return nil, conflicts
	}
	return out, nil
}

func writeMergedTree(ctx context.Context, api git.GitAPI, merged map[string]unit, base treeSnapshot, local treeSnapshot, remote treeSnapshot) (git.OID, error) {
	_, indexFile, cleanup, err := newTempIndex()
	if err != nil {
		return "", err
	}
	defer cleanup()

	if err := api.ReadTreeEmpty(ctx, indexFile); err != nil {
		return "", err
	}

	// Stage blobs for all merged units (including chunked parts).
	var paths []string
	type stage struct {
		path string
		oid  git.OID
	}
	var stages []stage

	for k, u := range merged {
		switch u.kind {
		case unitKindBlob:
			stages = append(stages, stage{path: k, oid: u.oid})
		case unitKindChunkedTree:
			parts := pickPartsForChunkedUnit(k, u, base, local, remote)
			for p, oid := range parts {
				stages = append(stages, stage{path: p, oid: oid})
			}
		default:
			// missing: no stage
		}
	}

	// Deterministic ordering.
	sort.Slice(stages, func(i, j int) bool { return stages[i].path < stages[j].path })
	paths = make([]string, 0, len(stages))
	for _, st := range stages {
		paths = append(paths, st.path)
		if err := api.UpdateIndexCacheInfo(ctx, indexFile, "100644", st.oid, st.path); err != nil {
			return "", err
		}
	}

	// Safety: ensure we didn't accidentally stage both "k" and "k/0001" (file/dir conflict).
	// The merge algorithm should prevent this, but keep a defensive check in one place.
	sort.Strings(paths)
	for i := 0; i+1 < len(paths); i++ {
		if strings.HasPrefix(paths[i+1], paths[i]+"/") {
			return "", fmt.Errorf("gitrebase: merge produced file/dir conflict: %q and %q", paths[i], paths[i+1])
		}
	}

	return api.WriteTree(ctx, indexFile)
}

func pickPartsForChunkedUnit(key string, u unit, base treeSnapshot, local treeSnapshot, remote treeSnapshot) map[string]git.OID {
	// Prefer exact-match on tree oid against one snapshot; otherwise fall back to u.parts.
	// The merge algorithm sets u.oid from one side, so this should resolve consistently.
	if v, ok := local.chunked[key]; ok && v.oid == u.oid {
		return v.parts
	}
	if v, ok := remote.chunked[key]; ok && v.oid == u.oid {
		return v.parts
	}
	if v, ok := base.chunked[key]; ok && v.oid == u.oid {
		return v.parts
	}
	return u.parts
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

func handleConflict(ctx context.Context, api git.GitAPI, localRef string, remoteTrackingRef string, base git.OID, localHead git.OID, remoteHead git.OID, conflict *git.MergeConflictError, msg string, author *git.Identity, hook ConflictHook) (newHead git.OID, updated bool, err error) {
	if hook == nil {
		return "", false, conflict
	}
	newHead, updated, handled, err := hook(ctx, api, localRef, remoteTrackingRef, base, localHead, remoteHead, conflict, msg, author)
	if err != nil {
		return "", false, err
	}
	if handled {
		return newHead, updated, nil
	}
	return "", false, conflict
}

func sortAndUniq(in []string) []string {
	if len(in) <= 1 {
		return in
	}
	sort.Strings(in)
	j := 0
	for i := 1; i < len(in); i++ {
		if in[i] == in[j] {
			continue
		}
		j++
		in[j] = in[i]
	}
	return in[:j+1]
}
