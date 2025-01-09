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
	return sql.RowsToRowIter(sql.UntypedSqlRow{ahead, behind}), nil
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
	headRef, err := rsr.CWBHeadRef()
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

	optCmt, err = doltdb.GetCommitAncestor(ctx, fromCommit, toCommit)
	if err != nil {
		return 0, 0, err
	}
	ancestor, ok := optCmt.ToCommit()
	if !ok {
		return 0, 0, doltdb.ErrGhostCommitEncountered
	}

	ancestorHash, err := ancestor.HashOf()
	if err != nil {
		return 0, 0, err
	}

	if fromHash != toHash {
		behind, err = countCommitsInRange(ctx, ddb, toHash, ancestorHash)
		if err != nil {
			return 0, 0, err
		}
		ahead, err = countCommitsInRange(ctx, ddb, fromHash, ancestorHash)
		if err != nil {
			return 0, 0, err
		}
	}

	return ahead, behind, nil
}

// countCommitsInRange returns the number of commits between the given starting point to trace back to the given target point.
// The starting commit must be a descendant of the target commit. Target commit must be a common ancestor commit.
func countCommitsInRange(ctx context.Context, ddb *doltdb.DoltDB, startCommitHash, targetCommitHash hash.Hash) (uint64, error) {
	itr, iErr := commitwalk.GetTopologicalOrderIterator(ctx, ddb, []hash.Hash{startCommitHash}, nil)
	if iErr != nil {
		return 0, iErr
	}
	count := 0
	for {
		nextHash, _, err := itr.Next(ctx)
		if err == io.EOF {
			return 0, fmt.Errorf("no match found to ancestor commit")
		} else if err != nil {
			return 0, err
		}

		if nextHash == targetCommitHash {
			break
		}
		count += 1
	}

	return uint64(count), nil
}
