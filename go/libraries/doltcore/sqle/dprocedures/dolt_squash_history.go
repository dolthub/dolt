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

package dprocedures

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// doltSquashHistory is the stored procedure for CALL dolt_squash_history(...). It collapses a
// suffix of the current branch's history into a single new commit that carries HEAD's tree,
// reparented onto the parents of the --first commit. This is a pointer rewrite, not a replay:
// it does not create new commits by replaying diffs, and is typically much faster than rebasing.
func doltSquashHistory(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	commitHash, err := doDoltSquashHistory(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(commitHash), nil
}

func doDoltSquashHistory(ctx *sql.Context, args []string) (string, error) {
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return "", fmt.Errorf("Empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return "", err
	}

	isReadOnly, err := isReadOnlyDatabase(ctx, dbName)
	if err != nil {
		return "", err
	}
	if isReadOnly {
		return "", fmt.Errorf("unable to squash history in read-only databases")
	}

	apr, err := cli.CreateSquashHistoryArgParser().Parse(args)
	if err != nil {
		return "", err
	}

	msg, ok := apr.GetValue(cli.MessageArg)
	if !ok || msg == "" {
		return "", fmt.Errorf("must provide a commit message with --%s", cli.MessageArg)
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return "", fmt.Errorf("Could not load database %s", dbName)
	}
	ddb := dbData.Ddb

	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return "", err
	}
	if ws.MergeActive() {
		return "", fmt.Errorf("cannot squash history while a merge is in progress; abort the merge first")
	}
	if ws.RebaseActive() {
		return "", fmt.Errorf("cannot squash history while a rebase is in progress; abort the rebase first")
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return "", fmt.Errorf("Could not load database %s", dbName)
	}
	hasChanges, _, _, err := actions.RootHasUncommittedChanges(roots)
	if err != nil {
		return "", err
	}
	if hasChanges {
		return "", fmt.Errorf("cannot squash history with uncommitted changes")
	}

	headRef, err := dbData.Rsr.CWBHeadRef(ctx)
	if err != nil {
		return "", err
	}
	headCommit, err := dSess.GetHeadCommit(ctx, dbName)
	if err != nil {
		return "", err
	}

	firstCommit, err := resolveFirstCommit(ctx, ddb, headRef, headCommit, apr)
	if err != nil {
		return "", err
	}
	// The initial commit has no parents to reparent onto and must survive as the permanent merge
	// base, so it can never be squashed away.
	if firstCommit.NumParents() == 0 {
		return "", fmt.Errorf("--%s cannot be the initial commit", cli.FirstParam)
	}

	newCommit, err := buildSquashedCommit(ctx, ddb, headCommit, firstCommit, msg)
	if err != nil {
		return "", err
	}

	if err = ddb.SetHeadToCommit(ctx, headRef, newCommit); err != nil {
		return "", err
	}

	// HEAD's tree is preserved, so the working and staged roots already match the new HEAD and need
	// no modification. Committing the transaction starts a fresh one so the session observes the
	// moved HEAD.
	if err = commitTransaction(ctx, dSess, nil); err != nil {
		return "", err
	}

	newHash, err := newCommit.HashOf()
	if err != nil {
		return "", err
	}
	return newHash.String(), nil
}

// resolveFirstCommit returns the oldest commit to include in the squash. When --first is supplied
// it is resolved and verified to be an ancestor of HEAD; otherwise it defaults to the initial
// commit's only child.
func resolveFirstCommit(ctx *sql.Context, ddb *doltdb.DoltDB, headRef ref.DoltRef, headCommit *doltdb.Commit, apr *argparser.ArgParseResults) (*doltdb.Commit, error) {
	firstArg, ok := apr.GetValue(cli.FirstParam)
	if !ok {
		return defaultFirstCommit(ctx, ddb, headCommit)
	}

	cs, err := doltdb.NewCommitSpec(firstArg)
	if err != nil {
		return nil, err
	}
	optCommit, err := ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return nil, err
	}
	firstCommit, ok := optCommit.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	firstHash, err := firstCommit.HashOf()
	if err != nil {
		return nil, err
	}
	optAncestor, err := doltdb.GetCommitAncestor(ctx, firstCommit, headCommit)
	if err != nil {
		return nil, err
	}
	if optAncestor.Addr != firstHash {
		return nil, fmt.Errorf("--%s must be an ancestor of HEAD", cli.FirstParam)
	}
	return firstCommit, nil
}

// defaultFirstCommit walks HEAD's ancestry, identifies the initial commit (the unique parentless
// commit), and returns its single child. If the initial commit has more than one child reachable
// from HEAD there is no unambiguous default, and the caller must supply --first.
func defaultFirstCommit(ctx *sql.Context, ddb *doltdb.DoltDB, headCommit *doltdb.Commit) (*doltdb.Commit, error) {
	headHash, err := headCommit.HashOf()
	if err != nil {
		return nil, err
	}
	ancestors, err := commitwalk.GetTopNTopoOrderedCommitsMatching(ctx, ddb, []hash.Hash{headHash}, -1,
		func(*doltdb.OptionalCommit) (bool, error) { return true, nil })
	if err != nil {
		return nil, err
	}

	var initHash hash.Hash
	initCount := 0
	for _, c := range ancestors {
		if c.NumParents() == 0 {
			if initHash, err = c.HashOf(); err != nil {
				return nil, err
			}
			initCount++
		}
	}
	if initCount != 1 {
		return nil, fmt.Errorf("cannot determine a unique initial commit; specify --%s", cli.FirstParam)
	}

	var children []*doltdb.Commit
	for _, c := range ancestors {
		parentHashes, err := c.ParentHashes(ctx)
		if err != nil {
			return nil, err
		}
		for _, ph := range parentHashes {
			if ph == initHash {
				children = append(children, c)
				break
			}
		}
	}
	switch len(children) {
	case 0:
		return nil, fmt.Errorf("nothing to squash: the branch contains only the initial commit")
	case 1:
		return children[0], nil
	default:
		return nil, fmt.Errorf("the initial commit has multiple children; specify --%s with the commit to squash from", cli.FirstParam)
	}
}

// buildSquashedCommit writes a dangling commit that carries |headCommit|'s tree and whose parents
// are exactly |firstCommit|'s parents, authored by the current session user with |msg|.
func buildSquashedCommit(ctx *sql.Context, ddb *doltdb.DoltDB, headCommit, firstCommit *doltdb.Commit, msg string) (*doltdb.Commit, error) {
	headRoot, err := headCommit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}
	_, valHash, err := ddb.WriteRootValue(ctx, headRoot)
	if err != nil {
		return nil, err
	}

	parents := make([]*doltdb.Commit, 0, firstCommit.NumParents())
	for i := 0; i < firstCommit.NumParents(); i++ {
		optParent, err := firstCommit.GetParent(ctx, i)
		if err != nil {
			return nil, err
		}
		parent, ok := optParent.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitEncountered
		}
		parents = append(parents, parent)
	}

	name, email, _, _, err := dsess.ResolveNameEmail(ctx, dsess.DoltAuthorName, dsess.DoltAuthorEmail)
	if err != nil {
		return nil, err
	}
	meta, err := datas.NewCommitMeta(name, email, msg)
	if err != nil {
		return nil, err
	}

	return ddb.CommitDanglingWithParentCommits(ctx, valHash, parents, meta)
}
