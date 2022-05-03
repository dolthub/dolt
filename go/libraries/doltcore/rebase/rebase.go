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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/hash"
)

type visitedSet map[hash.Hash]*doltdb.Commit

type NeedsRebaseFn func(ctx context.Context, cm *doltdb.Commit) (bool, error)

// EntireHistory returns a |NeedsRebaseFn| that rebases the entire commit history.
func EntireHistory() NeedsRebaseFn {
	return func(_ context.Context, cm *doltdb.Commit) (bool, error) {
		n, err := cm.NumParents()
		return n != 0, err
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

		n, err := cm.NumParents()
		if err != nil {
			return false, err
		}
		if n == 0 {
			return false, fmt.Errorf("commit %s is missing from the commit history of at least one rebase head", sh)
		}

		return true, nil
	}
}

type ReplayRootFn func(ctx context.Context, root, parentRoot, rebasedParentRoot *doltdb.RootValue) (rebaseRoot *doltdb.RootValue, err error)

type ReplayCommitFn func(ctx context.Context, commit, parent, rebasedParent *doltdb.Commit) (rebaseRoot *doltdb.RootValue, err error)

// wrapReplayRootFn converts a |ReplayRootFn| to a |ReplayCommitFn|
func wrapReplayRootFn(fn ReplayRootFn) ReplayCommitFn {
	return func(ctx context.Context, commit, parent, rebasedParent *doltdb.Commit) (rebaseRoot *doltdb.RootValue, err error) {
		root, err := commit.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}

		parentRoot, err := parent.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}

		rebasedParentRoot, err := rebasedParent.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}

		return fn(ctx, root, parentRoot, rebasedParentRoot)
	}
}

// AllBranches rewrites the history of all branches in the repo using the |replay| function.
func AllBranches(ctx context.Context, dEnv *env.DoltEnv, replay ReplayCommitFn, nerf NeedsRebaseFn) error {
	branches, err := dEnv.DoltDB.GetBranches(ctx)
	if err != nil {
		return err
	}

	return rebaseRefs(ctx, dEnv.DbData(), replay, nerf, branches...)
}

// CurrentBranch rewrites the history of the current branch using the |replay| function.
func CurrentBranch(ctx context.Context, dEnv *env.DoltEnv, replay ReplayCommitFn, nerf NeedsRebaseFn) error {
	return rebaseRefs(ctx, dEnv.DbData(), replay, nerf, dEnv.RepoStateReader().CWBHeadRef())
}

// AllBranchesByRoots rewrites the history of all branches in the repo using the |replay| function.
func AllBranchesByRoots(ctx context.Context, dEnv *env.DoltEnv, replay ReplayRootFn, nerf NeedsRebaseFn) error {
	branches, err := dEnv.DoltDB.GetBranches(ctx)
	if err != nil {
		return err
	}

	replayCommit := wrapReplayRootFn(replay)
	return rebaseRefs(ctx, dEnv.DbData(), replayCommit, nerf, branches...)
}

// CurrentBranchByRoot rewrites the history of the current branch using the |replay| function.
func CurrentBranchByRoot(ctx context.Context, dEnv *env.DoltEnv, replay ReplayRootFn, nerf NeedsRebaseFn) error {
	replayCommit := wrapReplayRootFn(replay)
	return rebaseRefs(ctx, dEnv.DbData(), replayCommit, nerf, dEnv.RepoStateReader().CWBHeadRef())
}

func rebaseRefs(ctx context.Context, dbData env.DbData, replay ReplayCommitFn, nerf NeedsRebaseFn, refs ...ref.DoltRef) error {
	ddb := dbData.Ddb
	rsr := dbData.Rsr
	rsw := dbData.Rsw
	drw := dbData.Drw

	cwbRef := rsr.CWBHeadRef()
	dd, err := drw.GetDocsOnDisk()
	if err != nil {
		return err
	}

	heads := make([]*doltdb.Commit, len(refs))
	for i, dRef := range refs {
		heads[i], err = ddb.ResolveCommitRef(ctx, dRef)
		if err != nil {
			return err
		}
	}

	newHeads, err := rebase(ctx, ddb, replay, nerf, heads...)
	if err != nil {
		return err
	}

	for i, dRef := range refs {

		switch dRef.(type) {
		case ref.BranchRef:
			err = ddb.NewBranchAtCommit(ctx, dRef, newHeads[i])
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("cannot rebase ref: %s", ref.String(dRef))
		}

		if err != nil {
			return err
		}
	}

	cm, err := ddb.ResolveCommitRef(ctx, cwbRef)
	if err != nil {
		return err
	}

	r, err := cm.GetRootValue(ctx)
	if err != nil {
		return err
	}

	_, err = doltdocs.UpdateRootWithDocs(ctx, r, dd)
	if err != nil {
		return err
	}

	// TODO: this should be a single update to repo state, not two
	err = rsw.UpdateStagedRoot(ctx, r)
	if err != nil {
		return err
	}

	return rsw.UpdateWorkingRoot(ctx, r)
}

func rebase(ctx context.Context, ddb *doltdb.DoltDB, replay ReplayCommitFn, nerf NeedsRebaseFn, origins ...*doltdb.Commit) ([]*doltdb.Commit, error) {
	var rebasedCommits []*doltdb.Commit
	vs := make(visitedSet)
	for _, cm := range origins {
		rc, err := rebaseRecursive(ctx, ddb, replay, nerf, vs, cm)

		if err != nil {
			return nil, err
		}

		rebasedCommits = append(rebasedCommits, rc)
	}

	return rebasedCommits, nil
}

func rebaseRecursive(ctx context.Context, ddb *doltdb.DoltDB, replay ReplayCommitFn, nerf NeedsRebaseFn, vs visitedSet, commit *doltdb.Commit) (*doltdb.Commit, error) {
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

	allParents, err := ddb.ResolveAllParents(ctx, commit)
	if err != nil {
		return nil, err
	}

	if len(allParents) < 1 {
		panic(fmt.Sprintf("commit: %s has no parents", commitHash.String()))
	}

	var allRebasedParents []*doltdb.Commit
	for _, p := range allParents {
		rp, err := rebaseRecursive(ctx, ddb, replay, nerf, vs, p)

		if err != nil {
			return nil, err
		}

		allRebasedParents = append(allRebasedParents, rp)
	}

	rebasedRoot, err := replay(ctx, commit, allParents[0], allRebasedParents[0])
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
