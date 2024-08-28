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
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/cherry_pick"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

var ErrEmptyCherryPick = errors.New("cannot cherry-pick empty string")

var cherryPickSchema = []*sql.Column{
	{
		Name:     "hash",
		Type:     gmstypes.LongText,
		Nullable: true,
	},
	{
		Name:     "data_conflicts",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
	{
		Name:     "schema_conflicts",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
	{
		Name:     "constraint_violations",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
}

// doltCherryPick is the stored procedure version for the CLI command `dolt cherry-pick`.
func doltCherryPick(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	newCommitHash, dataConflicts, schemaConflicts, constraintViolations, err := doDoltCherryPick(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(newCommitHash, int64(dataConflicts), int64(schemaConflicts), int64(constraintViolations)), nil
}

// doDoltCherryPick attempts to perform a cherry-pick merge based on the arguments specified in |args| and returns
// the new, created commit hash (if it was successful created), a count of the number of tables with data conflicts,
// a count of the number of tables with schema conflicts, and a count of the number of tables with constraint violations.
func doDoltCherryPick(ctx *sql.Context, args []string) (string, int, int, int, error) {
	// Get the information for the sql context.
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return "", 0, 0, 0, fmt.Errorf("error: empty database name")
	}

	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return "", 0, 0, 0, err
	}

	apr, err := cli.CreateCherryPickArgParser().Parse(args)
	if err != nil {
		return "", 0, 0, 0, err
	}

	if apr.Contains(cli.AbortParam) {
		return "", 0, 0, 0, cherry_pick.AbortCherryPick(ctx, dbName)
	}

	// we only support cherry-picking a single commit for now.
	if apr.NArg() == 0 {
		return "", 0, 0, 0, ErrEmptyCherryPick
	} else if apr.NArg() > 1 {
		return "", 0, 0, 0, fmt.Errorf("cherry-picking multiple commits is not supported yet")
	}

	cherryStr := apr.Arg(0)
	if len(cherryStr) == 0 {
		return "", 0, 0, 0, ErrEmptyCherryPick
	}

	cherryPickOptions := cherry_pick.NewCherryPickOptions()

	// If --allow-empty is specified, then empty commits are allowed to be cherry-picked
	if apr.Contains(cli.AllowEmptyFlag) {
		cherryPickOptions.EmptyCommitHandling = doltdb.KeepEmptyCommit
	}

	commit, mergeResult, err := cherry_pick.CherryPick(ctx, cherryStr, cherryPickOptions)
	if err != nil {
		return "", 0, 0, 0, err
	}

	if mergeResult != nil {
		return "",
			mergeResult.CountOfTablesWithDataConflicts(),
			mergeResult.CountOfTablesWithSchemaConflicts(),
			mergeResult.CountOfTablesWithConstraintViolations(),
			nil
	}

	return commit, 0, 0, 0, nil
}
