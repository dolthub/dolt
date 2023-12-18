// Copyright 2023 Dolthub, Inc.
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
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
)

const (
	RebaseActionPick   = "pick"
	RebaseActionSquash = "squash"
	RebaseActionFixup  = "fixup"
	RebaseActionDrop   = "drop"
	RebaseActionReword = "reword"
)

// ErrInvalidRebasePlanSquashFixupWithoutPick is returned when a rebase plan attempts to squash or
// fixup a commit without first picking or rewording a commit.
var ErrInvalidRebasePlanSquashFixupWithoutPick = fmt.Errorf("invalid rebase plan: squash and fixup actions must appear after a pick or reword action")

// RebasePlanDatabase is a database that can save and load a rebase plan.
type RebasePlanDatabase interface {
	// SaveRebasePlan saves the given rebase plan to the database.
	SaveRebasePlan(ctx *sql.Context, plan *RebasePlan) error
	// LoadRebasePlan loads the rebase plan from the database.
	LoadRebasePlan(ctx *sql.Context) (*RebasePlan, error)
}

// RebasePlan describes the plan for a rebase operation, where commits are reordered,
// or adjusted, and then replayed on top of a base commit to form a new commit history.
type RebasePlan struct {
	Members []RebasePlanMember
}

// RebasePlanMember describes a single step in a rebase plan, such as dropping a
// commit, squashing a commit into the prevous commit, etc.
type RebasePlanMember struct {
	// TODO: If we change the schema to be a DECIMAL(6,2), uint won't work anymore...
	//       Although... do we even need this field if the ordering is encapsulated
	//       by this field's position in the Members slice?
	RebaseOrder uint
	Action      string // TODO: how to easily sync up this action with the enum types?
	CommitHash  string
	CommitMsg   string
}

// CreateDefaultRebasePlan creates and returns the default rebase plan for the commits between
// |startCommit| and |upstreamCommit|, equivalent to the log of startCommit..upstreamCommit. The
// default plan includes each of those commits, in the same order they were originally applied, and
// each step in the plan will have the default, pick, action. If the plan cannot be generated for
// any reason, such as disconnected or invalid commits specified, then an error is returned.
func CreateDefaultRebasePlan(ctx *sql.Context, startCommit, upstreamCommit *doltdb.Commit) (*RebasePlan, error) {
	commits, err := findRebaseCommits(ctx, startCommit, upstreamCommit)
	if err != nil {
		return nil, err
	}

	if len(commits) == 0 {
		return nil, fmt.Errorf("didn't identify any commits!")
	}

	plan := RebasePlan{}
	for idx := len(commits) - 1; idx >= 0; idx-- {
		commit := commits[idx]
		hash, err := commit.HashOf()
		if err != nil {
			return nil, err
		}
		meta, err := commit.GetCommitMeta(ctx)
		if err != nil {
			return nil, err
		}

		plan.Members = append(plan.Members, RebasePlanMember{
			RebaseOrder: uint(len(commits) - idx),
			Action:      RebaseActionPick,
			CommitHash:  hash.String(), // TODO: Maybe keep as a hash.Hash instance?
			CommitMsg:   meta.Description,
		})
	}

	return &plan, nil
}

// ValidateRebasePlan returns a validation error for invalid states in a rebase plan, such as
// squash or fixup actions appearing in the plan before a pick or reword action.
func ValidateRebasePlan(ctx *sql.Context, plan *RebasePlan) error {
	seenPick := false
	seenReword := false
	for i, step := range plan.Members {
		// As a sanity check, make sure the rebase order is ascending. This shouldn't EVER happen because the
		// results are sorted from the database query, but double check while we're validating the plan.
		if i > 0 && plan.Members[i-1].RebaseOrder >= step.RebaseOrder {
			return fmt.Errorf("invalid rebase plan: rebase order must be ascending")
		}

		switch step.Action {
		case RebaseActionPick:
			seenPick = true

		case RebaseActionReword:
			seenReword = true

		case RebaseActionFixup, RebaseActionSquash:
			if !seenPick && !seenReword {
				return ErrInvalidRebasePlanSquashFixupWithoutPick
			}
		}

		if err := validateCommit(ctx, step.CommitHash); err != nil {
			return err
		}
	}

	return nil
}

// validateCommit returns an error if the specified |commit| is not able to be resolved.
func validateCommit(ctx *sql.Context, commit string) error {
	doltSession := dsess.DSessFromSess(ctx.Session)

	ddb, ok := doltSession.GetDoltDB(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return fmt.Errorf("unable to load dolt db")
	}

	if !doltdb.IsValidCommitHash(commit) {
		return fmt.Errorf("invalid commit hash: %s", commit)
	}

	commitSpec, err := doltdb.NewCommitSpec(commit)
	if err != nil {
		return err
	}
	_, err = ddb.Resolve(ctx, commitSpec, nil)
	if err != nil {
		return fmt.Errorf("unable to resolve commit hash %s: %w", commit, err)
	}

	return nil
}

// findRebaseCommits returns the commits that should be included in the default rebase plan when
// rebasing |upstreamBranchCommit| onto the current branch (specified by commit |currentBranchCommit|).
// This is defined as the log of |currentBranchCommit|..|upstreamBranchCommit|, or in other words, the
// commits that are reachable from the current branch HEAD, but are NOT reachable from
// |upstreamBranchCommit|.
func findRebaseCommits(ctx *sql.Context, currentBranchCommit, upstreamBranchCommit *doltdb.Commit) (commits []*doltdb.Commit, err error) {
	doltSession := dsess.DSessFromSess(ctx.Session)

	ddb, ok := doltSession.GetDoltDB(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return nil, fmt.Errorf("unable to load dolt db")
	}

	currentBranchCommitHash, err := currentBranchCommit.HashOf()
	if err != nil {
		return
	}

	upstreamBranchCommitHash, err := upstreamBranchCommit.HashOf()
	if err != nil {
		return
	}

	// We use the dot-dot revision iterator because it gives us the behavior we want for rebase – it finds all
	// commits reachable from |currentBranchCommit| but NOT reachable by |upstreamBranchCommit|.
	commitItr, err := commitwalk.GetDotDotRevisionsIterator(ctx,
		ddb, []hash.Hash{currentBranchCommitHash},
		ddb, []hash.Hash{upstreamBranchCommitHash}, nil)
	if err != nil {
		return nil, err
	}

	// TODO: Currently, we drain the iterator into a slice so that we can see the total
	// number of commits and use that to set the rebase_order when we create the dolt_rebase
	// table. This is easier, but could be optimized if we had a way to generate this same
	// set of commits in reverse order.
	for {
		_, commit, err := commitItr.Next(ctx)
		if err == io.EOF {
			return commits, nil
		} else if err != nil {
			return nil, err
		}

		commits = append(commits, commit)
	}
}
