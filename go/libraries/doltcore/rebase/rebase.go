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

// TODO: These types can't live in here because of an import cycle :-(
//type RebasePlan struct {
//	Members []RebasePlanMember
//}
//
//type RebasePlanMember struct {
//	RebaseOrder  uint // TODO: If we change the schema to be a DECIMAL(6,2), uint won't work anymore...
//	Action       string // TODO: how to easily sync up this action with the enum types?
//	CommitHash   string
//	CommitMsg    string
//}

func CreateRebasePlan(ctx *sql.Context, startCommit, upstreamCommit *doltdb.Commit) (*doltdb.RebasePlan, error) {
	commits, err := findRebaseCommits(ctx, startCommit, upstreamCommit)
	if err != nil {
		return nil, err
	}

	if len(commits) == 0 {
		return nil, fmt.Errorf("didn't identify any commits!")
	}

	plan := doltdb.RebasePlan{}
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

		plan.Members = append(plan.Members, doltdb.RebasePlanMember{
			// TODO: Once we have these loaded in to a plan structure, it would be easy to adjust the order, too.
			RebaseOrder: uint(len(commits)) - uint(idx),
			Action:      RebaseActionPick,
			CommitHash:  hash.String(), // TODO: Maybe keep as a hash.Hash instance?
			CommitMsg:   meta.Description,
		})
	}

	return &plan, nil
}

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
