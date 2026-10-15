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

package dprocedures

import (
	"context"
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
)

func doltCountCommits(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	ahead, behind, err := countCommits(ctx, args...)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{ahead, behind}), nil
}

func countCommits(ctx *sql.Context, args ...string) (ahead uint64, behind uint64, err error) {
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return 0, 0, fmt.Errorf("empty database name")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	apr, err := cli.CreateCountCommitsArgParser().Parse(args)
	if err != nil {
		return 0, 0, err
	}
	fromRef, ok := apr.GetValue("from")
	if !ok {
		return 0, 0, fmt.Errorf("missing from ref")
	}
	if len(fromRef) == 0 {
		return 0, 0, fmt.Errorf("empty from ref")
	}
	toRef, ok := apr.GetValue("to")
	if !ok {
		return 0, 0, fmt.Errorf("missing to ref")
	}
	if len(toRef) == 0 {
		return 0, 0, fmt.Errorf("empty to ref")
	}

	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return 0, 0, fmt.Errorf("could not load database %s", dbName)
	}
	ddb := dbData.Ddb
	rsr := dbData.Rsr

	fromSpec, err := doltdb.NewCommitSpec(fromRef)
	if err != nil {
		return 0, 0, err
	}
	headRef, err := rsr.CWBHeadRef(ctx)
	if err != nil {
		return 0, 0, err
	}
	optCmt, err := ddb.Resolve(ctx, fromSpec, headRef)
	if err != nil {
		return 0, 0, err
	}
	fromCommit, ok := optCmt.ToCommit()
	if !ok {
		return 0, 0, doltdb.ErrGhostCommitEncountered
	}

	fromHash, err := fromCommit.HashOf()
	if err != nil {
		return 0, 0, err
	}

	toSpec, err := doltdb.NewCommitSpec(toRef)
	if err != nil {
		return 0, 0, err
	}
	optCmt, err = ddb.Resolve(ctx, toSpec, headRef)
	if err != nil {
		return 0, 0, err
	}
	toCommit, ok := optCmt.ToCommit()
	if !ok {
		return 0, 0, doltdb.ErrGhostCommitEncountered
	}

	toHash, err := toCommit.HashOf()
	if err != nil {
		return 0, 0, err
	}

	// Unrelated histories are an error, rather than counting every commit on each side.
	if _, err = doltdb.GetCommitAncestor(ctx, fromCommit, toCommit); err != nil {
		return 0, 0, err
	}

	if fromHash != toHash {
		ahead, err = countCommitsExcluding(ctx, ddb, fromHash, toHash)
		if err != nil {
			return 0, 0, err
		}
		behind, err = countCommitsExcluding(ctx, ddb, toHash, fromHash)
		if err != nil {
			return 0, 0, err
		}
	}

	return ahead, behind, nil
}

// countCommitsExcluding returns the number of commits reachable from |include| but not from |exclude|, the count of
// `git rev-list exclude..include`.
func countCommitsExcluding(ctx context.Context, ddb *doltdb.DoltDB, include, exclude hash.Hash) (uint64, error) {
	itr, err := commitwalk.GetDotDotRevisionsIterator[context.Context](ctx, ddb, []hash.Hash{include}, ddb, []hash.Hash{exclude}, nil)
	if err != nil {
		return 0, err
	}
	var count uint64
	for {
		_, _, _, _, err = itr.Next(ctx)
		if err == io.EOF {
			return count, nil
		} else if err != nil {
			return 0, err
		}
		count++
	}
}
