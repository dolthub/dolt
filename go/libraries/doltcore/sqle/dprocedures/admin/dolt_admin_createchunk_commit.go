// Copyright 2025 Dolthub, Inc.
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

package admin

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// rowToIter returns a sql.RowIter with a single row containing the values passed in.
func rowToIter(vals ...interface{}) sql.RowIter {
	row := make(sql.Row, len(vals))
	for i, val := range vals {
		row[i] = val
	}
	return sql.RowsToRowIter(row)
}

func CreateCommit(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return nil, fmt.Errorf("empty database name")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return nil, err
	}

	apr, err := cli.CreateCreateCommitParser().Parse(args)
	if err != nil {
		return nil, err
	}

	desc, _ := apr.GetValue("desc")
	root, _ := apr.GetValue("root")
	parents, _ := apr.GetValueList("parents")
	branch, isBranchSet := apr.GetValue(cli.BranchParam)
	force := apr.Contains(cli.ForceFlag)

	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return nil, err
		}
	} else {
		// In SQL mode, use the current SQL user as the commit author, instead of the `dolt config` configured values.
		// We won't have an email address for the SQL user though, so instead use the MySQL user@address notation.
		name = ctx.Client().User
		email = fmt.Sprintf("%s@%s", ctx.Client().User, ctx.Client().Address)
	}

	dSess := dsess.DSessFromSess(ctx.Session)

	dbData, ok := dSess.GetDbData(ctx, dbName)
	db := dbData.Ddb
	commitRootHash, ok := hash.MaybeParse(root)
	if !ok {
		return nil, fmt.Errorf("invalid root value hash")
	}

	var parentCommits []hash.Hash
	for _, parent := range parents {
		commitSpec, err := doltdb.NewCommitSpec(parent)
		if err != nil {
			return nil, err
		}

		headRef, err := dSess.CWBHeadRef(ctx, dbName)
		if err != nil {
			return nil, err
		}

		optionalCommit, err := db.Resolve(ctx, commitSpec, headRef)
		if err != nil {
			return nil, err
		}
		parentCommits = append(parentCommits, optionalCommit.Addr)
	}

	commitMeta, err := datas.NewCommitMeta(name, email, desc)
	if err != nil {
		return nil, err
	}

	// This isn't technically an amend, but the Amend field controls whether the commit must be a child of the ref's current commit (if any)
	commitOpts := datas.CommitOptions{
		Parents: parentCommits,
		Meta:    commitMeta,
		Amend:   force,
	}

	rootVal, err := dbData.Ddb.ValueReadWriter().ReadValue(ctx, commitRootHash)
	if err != nil {
		return nil, err
	}

	var commit *doltdb.Commit
	if isBranchSet {
		commit, err = dbData.Ddb.CommitValue(ctx, ref.NewBranchRef(branch), rootVal, commitOpts)
		if errors.Is(err, datas.ErrMergeNeeded) {
			return nil, fmt.Errorf("branch %s already exists. If you wish to overwrite it, add the --force flag", branch)
		}
	} else {
		commit, err = dbData.Ddb.CommitDangling(ctx, rootVal, commitOpts)
	}
	if err != nil {
		return nil, err
	}

	commitHash, err := commit.HashOf()
	if err != nil {
		return nil, err
	}
	return rowToIter(commitHash.String()), nil
}
