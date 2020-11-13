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

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/hash"
)

type visitedSet map[hash.Hash]*doltdb.Commit

type ReplayCommitFn func(ctx context.Context, root, parentRoot, rebasedParentRoot *doltdb.RootValue) (rebaseRoot *doltdb.RootValue, err error)

type NeedsRebaseFn func(ctx context.Context, cm *doltdb.Commit) (bool, error)

func EntireHistory(_ context.Context, cm *doltdb.Commit) (bool, error) {
	n, err := cm.NumParents()
	return n != 0, err
}

func GetHeadCommits(ctx context.Context, dEnv *env.DoltEnv) (heads []*doltdb.Commit, branches []ref.DoltRef, err error) {
	branches, err = dEnv.DoltDB.GetBranches(ctx)
	if err != nil {
		return nil, nil, err
	}

	heads = make([]*doltdb.Commit, len(branches))
	for i, dRef := range branches {

		cs, err := doltdb.NewCommitSpec(dRef.String())

		if err != nil {
			return nil, nil, err
		}

		heads[i], err = dEnv.DoltDB.Resolve(ctx, cs, nil)
		if err != nil {
			return nil, nil, err
		}
	}
	return
}

// MigrateUniqueTags rebases the history of the repo to uniquify tags within branch histories.
func AllBranches(ctx context.Context, dEnv *env.DoltEnv, replay ReplayCommitFn, nerf NeedsRebaseFn) error {
	ddb := dEnv.DoltDB
	cwbSpec := dEnv.RepoState.CWBHeadSpec()
	cwbRef := dEnv.RepoState.CWBHeadRef()
	dd, err := dEnv.GetAllValidDocDetails()
	if err != nil {
		return err
	}

	headCommits, branches, err := GetHeadCommits(ctx, dEnv)
	if err != nil {
		return err
	}

	newCommits, err := rebase(ctx, ddb, replay, nerf, headCommits...)
	if err != nil {
		return err
	}

	for idx, dRef := range branches {

		err = ddb.DeleteBranch(ctx, dRef)
		if err != nil {
			return err
		}

		err = ddb.NewBranchAtCommit(ctx, dRef, newCommits[idx])
		if err != nil {
			return err
		}
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cwbSpec, cwbRef)
	if err != nil {
		return err
	}

	r, err := cm.GetRootValue()
	if err != nil {
		return err
	}

	_, err = dEnv.UpdateStagedRoot(ctx, r)
	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingRoot(ctx, r)
	if err != nil {
		return err
	}

	err = dEnv.PutDocsToWorking(ctx, dd)
	if err != nil {
		return err
	}

	_, err = dEnv.PutDocsToStaged(ctx, dd)
	return err
}

// MigrateUniqueTags rebases the history of the repo to uniquify tags within branch histories.
func CurrentBranch(ctx context.Context, dEnv *env.DoltEnv, replay ReplayCommitFn, nerf NeedsRebaseFn) error {
	ddb := dEnv.DoltDB
	cwbSpec := dEnv.RepoState.CWBHeadSpec()
	cwbRef := dEnv.RepoState.CWBHeadRef()
	dd, err := dEnv.GetAllValidDocDetails()
	if err != nil {
		return err
	}

	head, err := dEnv.DoltDB.Resolve(ctx, cwbSpec, cwbRef)
	if err != nil {
		return err
	}

	newCommits, err := rebase(ctx, ddb, replay, nerf, head)
	if err != nil {
		return err
	}

	err = ddb.DeleteBranch(ctx, cwbRef)
	if err != nil {
		return err
	}

	err = ddb.NewBranchAtCommit(ctx, cwbRef, newCommits[0])
	if err != nil {
		return err
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cwbSpec, cwbRef)
	if err != nil {
		return err
	}

	r, err := cm.GetRootValue()
	if err != nil {
		return err
	}

	_, err = dEnv.UpdateStagedRoot(ctx, r)
	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingRoot(ctx, r)
	if err != nil {
		return err
	}

	err = dEnv.PutDocsToWorking(ctx, dd)
	if err != nil {
		return err
	}

	_, err = dEnv.PutDocsToStaged(ctx, dd)
	return err
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

	root, err := commit.GetRootValue()

	if err != nil {
		return nil, err
	}

	parentRoot, err := allParents[0].GetRootValue()

	if err != nil {
		return nil, err
	}

	// we can diff off of any parent
	rebasedParentRoot, err := allRebasedParents[0].GetRootValue()

	if err != nil {
		return nil, err
	}

	rebasedRoot, err := replay(ctx, root, parentRoot, rebasedParentRoot)

	if err != nil {
		return nil, err
	}

	valueHash, err := ddb.WriteRootValue(ctx, rebasedRoot)

	if err != nil {
		return nil, err
	}

	oldMeta, err := commit.GetCommitMeta()

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
