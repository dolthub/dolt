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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var doltRebaseProcedureSchema = []*sql.Column{
	{
		Name:     "status",
		Type:     types.Int64,
		Nullable: false,
	},
	{
		Name:     "message",
		Type:     types.LongText,
		Nullable: true,
	},
}

var RebaseActionEnumType = types.MustCreateEnumType([]string{"pick", "skip", "squash"}, sql.Collation_Default)

var DoltRebaseSystemTableSchema = []*sql.Column{
	{

		Name:     "rebase_order",
		Type:     types.Uint16,
		Nullable: false,
	},
	{
		Name:     "action",
		Type:     RebaseActionEnumType,
		Nullable: false,
	},
	{
		Name:     "commit_hash",
		Type:     types.Text,
		Nullable: false,
	},
	{
		Name:     "commit_message",
		Type:     types.Text,
		Nullable: false,
	},
}

func doltRebase(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltRebase(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltRebase(ctx *sql.Context, args []string) (int, error) {
	// TODO: Set working set metadata for active rebase (similar to merge metadata)
	//       - how does this work for merge again?

	if len(args) > 1 {
		return 1, fmt.Errorf("too many args")
	}

	if len(args) == 1 {
		if strings.ToLower(args[0]) == "--abort" {
			err := abortRebase(ctx)
			if err != nil {
				return 1, err
			} else {
				return 0, nil
			}
		} else if strings.ToLower(args[0]) == "--continue" {
			continueRebase(ctx)
		}
	}

	if len(args) == 0 {
		err := startRebase(ctx)
		if err != nil {
			return 1, err
		} else {
			return 0, nil
		}
	}

	return 0, nil
}

func startRebase(ctx *sql.Context) error {
	doltSession := dsess.DSessFromSess(ctx.Session)

	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	// TODO: For now, just rewind back a couple of commits...

	headRef, err := doltSession.CWBHeadRef(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	dbData, ok := doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		panic("not okay getting database!")
	}

	commitSpec, err := doltdb.NewCommitSpec("HEAD~~~")
	if err != nil {
		return err
	}

	commit, err := dbData.Ddb.Resolve(ctx, commitSpec, headRef)
	if err != nil {
		return err
	}

	newWorkingSet := workingSet.StartRebase(commit)
	err = doltSession.SetWorkingSet(ctx, ctx.GetCurrentDatabase(), newWorkingSet)
	if err != nil {
		return err
	}

	// TODO: Create dolt_rebase table
	//         - find all commits between branch HEAD and rebase start commit
	//         - create dolt_rebase table

	// TODO: What's the right way to create the dolt_reset system table... seems like we want it to be a real table, but
	//       we don't want to allow it to be checked in and committed, right?
	//       If it were a traditional, read-only system table, we'd probably regenerate it each time it was queried, which
	//       isn't the behavior we want. We want it generated once and then let the customer change it. Once we start
	//       executing the rebase plan, we CANNOT allow it to be changed though.

	dbData, ok = doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		panic("database not okay!")
	}

	db, err := doltSession.Provider().Database(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	rdb, ok := db.(dsess.RebaseableDatabase)
	if !ok {
		return fmt.Errorf("expected a *sqle.Database, but received a %T", db)
	}
	err = rdb.CreateRebasePlan(ctx, commit)
	if err != nil {
		return err
	}

	return nil
}

func abortRebase(ctx *sql.Context) error {
	doltSession := dsess.DSessFromSess(ctx.Session)

	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	if !workingSet.RebaseActive() {
		return fmt.Errorf("no active rebase")
	}

	// TODO: remove the dolt_rebase table

	workingSet = workingSet.AbortRebase()
	return doltSession.SetWorkingSet(ctx, ctx.GetCurrentDatabase(), workingSet)
}

func continueRebase(ctx *sql.Context) error {
	// TODO: validate the dolt_rebase table

	return nil
}
