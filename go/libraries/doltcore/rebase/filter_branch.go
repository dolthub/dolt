// Copyright 2020 Dolthub, Inc.
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

package rebase

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/hash"
)

type visitedSet map[hash.Hash]*doltdb.Commit

type NeedsRebaseFn func(ctx context.Context, cm *doltdb.Commit) (bool, error)

// EntireHistory returns a |NeedsRebaseFn| that rebases the entire commit history.
func EntireHistory() NeedsRebaseFn {
	return func(_ context.Context, cm *doltdb.Commit) (bool, error) {
		return cm.NumParents() != 0, nil
	}
}

// StopAtCommit returns a |NeedsRebaseFn| that rebases the commit history until
// |stopCommit| is reached. It will error if |stopCommit| is not reached.
func StopAtCommit(stopCommit *doltdb.Commit) NeedsRebaseFn {
	return func(ctx context.Context, cm *doltdb.Commit) (bool, error) {
		h, err := cm.HashOf()
		if err != nil {
			return false, err
		}

		sh, err := stopCommit.HashOf()
		if err != nil {
			return false, err
		}

		if h.Equal(sh) {
			return false, nil
		}

		if cm.NumParents() == 0 {
			return false, fmt.Errorf("commit %s is missing from the commit history of at least one rebase head", sh)
		}

		return true, nil
	}
}

// RootReplayer is something that takes a root value and rebases it with changes.
type RootReplayer interface {
	ReplayRoot(ctx context.Context, root, parentRoot, rebasedParentRoot doltdb.RootValue) (rebaseRoot doltdb.RootValue, err error)
}

// CommitReplayer is something that takes a commit and rebases it with changes.
type CommitReplayer interface {
	ReplayCommit(ctx context.Context, commit, parent, rebasedParent *doltdb.Commit) (rebaseRoot doltdb.RootValue, err error)
}

// AllBranchesAndTags rewrites the history of all branches and tags in the repo using the |replay| function.
func AllBranchesAndTags(ctx context.Context, dEnv *env.DoltEnv, applyUncommitted bool, commitReplayer CommitReplayer, rootReplayer RootReplayer, nerf NeedsRebaseFn) error {
	branches, err := dEnv.DoltDB(ctx).GetBranches(ctx)
	if err != nil {
		return err
	}
	tags, err := dEnv.DoltDB(ctx).GetTags(ctx)
	if err != nil {
		return err
	}
	return rebaseRefs(ctx, dEnv.DbData(ctx), applyUncommitted, commitReplayer, rootReplayer, nerf, append(branches, tags...)...)
}

// AllBranches rewrites the history of all branches in the repo using the |replay| function.
func AllBranches(ctx context.Context, dEnv *env.DoltEnv, applyUncommitted bool, commitReplayer CommitReplayer, rootReplayer RootReplayer, nerf NeedsRebaseFn) error {
	branches, err := dEnv.DoltDB(ctx).GetBranches(ctx)
	if err != nil {
		return err
	}
	return rebaseRefs(ctx, dEnv.DbData(ctx), applyUncommitted, commitReplayer, rootReplayer, nerf, branches...)
}

// CurrentBranch rewrites the history of the current branch using the |replay| function.
func CurrentBranch(ctx context.Context, dEnv *env.DoltEnv, applyUncommitted bool, commitReplayer CommitReplayer, rootReplayer RootReplayer, nerf NeedsRebaseFn) error {
	headRef, err := dEnv.RepoStateReader().CWBHeadRef(ctx)
	if err != nil {
		return nil
	}
	return rebaseRefs(ctx, dEnv.DbData(ctx), applyUncommitted, commitReplayer, rootReplayer, nerf, headRef)
}

func rebaseRefs(ctx context.Context, dbData env.DbData[context.Context], applyUncommitted bool, commitReplayer CommitReplayer, rootReplayer RootReplayer, nerf NeedsRebaseFn, refs ...ref.DoltRef) error {
	ddb := dbData.Ddb
	heads := make([]*doltdb.Commit, len(refs))
	for i, dRef := range refs {
		var err error
		heads[i], err = ddb.ResolveCommitRef(ctx, dRef)
		if err != nil {
			return err
		}
	}

	newWorkingSets := make([]*doltdb.WorkingSet, len(refs))
	for i, dRef := range refs {
		switch dRef.(type) {
		case ref.BranchRef:
			hRootVal, err := heads[i].GetRootValue(ctx)
			if err != nil {
				return err
			}
			hHash, err := hRootVal.HashOf()
			if err != nil {
				return err
			}

			wsRef, err := ref.WorkingSetRefForHead(dRef)
			if err != nil {
				return err
			}
			ws, err := ddb.ResolveWorkingSet(ctx, wsRef)
			if err != nil {
				return err
			}
			wHash, err := ws.WorkingRoot().HashOf()
			if err != nil {
				return err
			}
			sHash, err := ws.StagedRoot().HashOf()
			if err != nil {
				return err
			}
			if !applyUncommitted && (!hHash.Equal(wHash) || !hHash.Equal(sHash)) {
				return fmt.Errorf("local changes detected on branch %s, clear uncommitted changes (dolt stash dolt commit) before using filter-branch, or use --apply-to-uncommitted", dRef.String())
			}

			if !hHash.Equal(wHash) {
				var newWRoot doltdb.RootValue
				newWRoot, err = rootReplayer.ReplayRoot(ctx, ws.WorkingRoot(), nil, nil)
				if err != nil {
					return err
				}
				ws = ws.WithWorkingRoot(newWRoot)
			} else {
				ws = ws.WithWorkingRoot(nil)
			}
			if !hHash.Equal(sHash) {
				var newSRoot doltdb.RootValue
				newSRoot, err = rootReplayer.ReplayRoot(ctx, ws.StagedRoot(), nil, nil)
				if err != nil {
					return err
				}
				ws = ws.WithStagedRoot(newSRoot)
			} else {
				ws = ws.WithStagedRoot(nil)
			}
			newWorkingSets[i] = ws
		default:
			newWorkingSets[i] = nil
		}
	}

	newHeads, err := rebase(ctx, ddb, commitReplayer, nerf, heads...)
	if err != nil {
		return err
	}

	for i, r := range refs {
		switch dRef := r.(type) {
		case ref.BranchRef:
			newHead := newHeads[i]
			err = ddb.NewBranchAtCommit(ctx, dRef, newHead, nil)
			if err != nil {
				return err
			}

			newWorkingSet := newWorkingSets[i]
			if newWorkingSet == nil {
				continue
			}

			var wsRef ref.WorkingSetRef
			wsRef, err = ref.WorkingSetRefForHead(dRef)
			if err != nil {
				return err
			}

			var ws *doltdb.WorkingSet
			ws, err = ddb.ResolveWorkingSet(ctx, wsRef)
			if err != nil {
				return err
			}

			if newWorkingSet.WorkingRoot() != nil {
				ws = ws.WithWorkingRoot(newWorkingSet.WorkingRoot())
			}
			if newWorkingSet.StagedRoot() != nil {
				ws = ws.WithStagedRoot(newWorkingSet.StagedRoot())
			}

			var currWsHash hash.Hash
			currWsHash, err = ws.HashOf()
			if err != nil {
				return err
			}

			err = ddb.UpdateWorkingSet(ctx, wsRef, ws, currWsHash, ws.Meta(), nil)
		case ref.TagRef:
			// rewrite tag with new commit
			var tag *doltdb.Tag
			if tag, err = ddb.ResolveTag(ctx, dRef); err != nil {
				return err
			}
			if err = ddb.DeleteTag(ctx, dRef); err != nil {
				return err
			}
			err = ddb.NewTagAtCommit(ctx, dRef, newHeads[i], tag.Meta)
		default:
			return fmt.Errorf("cannot rebase ref: %s", ref.String(dRef))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func rebase(ctx context.Context, ddb *doltdb.DoltDB, commitReplayer CommitReplayer, nerf NeedsRebaseFn, origins ...*doltdb.Commit) ([]*doltdb.Commit, error) {
	var rebasedCommits []*doltdb.Commit
	vs := make(visitedSet)
	for _, cm := range origins {
		rc, err := rebaseRecursive(ctx, ddb, commitReplayer, nerf, vs, cm)

		if err != nil {
			return nil, err
		}

		rebasedCommits = append(rebasedCommits, rc)
	}

	return rebasedCommits, nil
}

func rebaseRecursive(ctx context.Context, ddb *doltdb.DoltDB, commitReplayer CommitReplayer, nerf NeedsRebaseFn, vs visitedSet, commit *doltdb.Commit) (*doltdb.Commit, error) {
	commitHash, err := commit.HashOf()
	if err != nil {
		return nil, err
	}
	visitedCommit, found := vs[commitHash]
	if found {
		// base case: reached previously rebased node
		return visitedCommit, nil
	}

	needToRebase, err := nerf(ctx, commit)
	if err != nil {
		return nil, err
	}
	if !needToRebase {
		// base case: reached bottom of DFS,
		return commit, nil
	}

	allOptParents, err := ddb.ResolveAllParents(ctx, commit)
	if err != nil {
		return nil, err
	}

	if len(allOptParents) < 1 {
		panic(fmt.Sprintf("commit: %s has no parents", commitHash.String()))
	}

	// convert allOptParents to allParents
	var allParents []*doltdb.Commit
	for _, optParent := range allOptParents {
		parent, ok := optParent.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitEncountered
		}
		allParents = append(allParents, parent)
	}

	var allRebasedParents []*doltdb.Commit
	for _, p := range allParents {
		rp, err := rebaseRecursive(ctx, ddb, commitReplayer, nerf, vs, p)

		if err != nil {
			return nil, err
		}

		allRebasedParents = append(allRebasedParents, rp)
	}

	rebasedRoot, err := commitReplayer.ReplayCommit(ctx, commit, allParents[0], allRebasedParents[0])
	if err != nil {
		return nil, err
	}

	r, valueHash, err := ddb.WriteRootValue(ctx, rebasedRoot)
	if err != nil {
		return nil, err
	}
	rebasedRoot = r

	oldMeta, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	rebasedCommit, err := ddb.CommitDanglingWithParentCommits(ctx, valueHash, allRebasedParents, oldMeta)
	if err != nil {
		return nil, err
	}

	vs[commitHash] = rebasedCommit
	return rebasedCommit, nil
}
