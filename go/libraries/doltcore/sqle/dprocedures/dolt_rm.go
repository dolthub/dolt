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

package dprocedures

import (
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/go-mysql-server/sql"
)

// doltRm is the stored procedure for the NOT YET IMPLEMENTED cli command dolt rm
func doltRm(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltRm(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltRm(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return 1, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	_, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	apr, err := cli.CreateRmArgParser().Parse(args)
	if err != nil {
		return 1, err
	}
	if apr.NArg() < 1 {
		return 1, fmt.Errorf("Nothing specified, nothing removed. Which tables should I remove?")
	}

	// Disallow manipulating any roots for read-only databases
	isReadOnly, err := isReadOnlyDatabase(ctx, dbName)
	if err != nil {
		return 1, err
	}
	if isReadOnly {
		return 1, fmt.Errorf("unable to rm in read-only databases")
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	checkStaged := apr.Contains(cli.CachedFlag)
	verifiedTables, err := verifyTables(ctx, apr.Args, checkStaged, roots)
	if err != nil {
		return 1, err
	}

	roots.Staged, err = roots.Staged.RemoveTables(ctx, false, false, verifiedTables...)
	if err != nil {
		return 1, err
	}

	// If --cached was not used, we want to fully delete the tables, so we remove them from the working set as well.
	if !checkStaged {
		roots.Working, err = roots.Working.RemoveTables(ctx, false, false, verifiedTables...)
		if err != nil {
			return 1, err
		}
	}

	if err = dSess.SetRoots(ctx, dbName, roots); err != nil {
		return 1, err
	}
	if err = commitTransaction(ctx, dSess, nil); err != nil {
		return 1, err
	}

	return 0, nil
}

func verifyTables(ctx *sql.Context, unqualifiedTables []string, checkStaged bool, roots doltdb.Roots) ([]doltdb.TableName, error) {
	var missingTables []string
	var missingStaged []string
	var TableNames []doltdb.TableName

	for _, name := range unqualifiedTables {
		_, okHead, err := resolve.TableName(ctx, roots.Head, name)
		if err != nil {
			return nil, err
		}
		tblName, okStage, err := resolve.TableName(ctx, roots.Staged, name)
		if err != nil {
			return nil, err
		}

		// If tables exist in both staged and head roots, we can remove them
		// If it is not in the head, we either error if --cached was not used, or add it otherwise
		// If neither is true it does not exist in staged, or does not exist in both.
		// In this case there's nothing to remove, so we error.
		if okHead && okStage {
			TableNames = append(TableNames, tblName)
		} else if okStage {
			if checkStaged {
				TableNames = append(TableNames, tblName)
			} else {
				missingStaged = append(missingStaged, name)
			}
		} else {
			missingTables = append(missingTables, name)
		}
	}

	if len(missingTables) > 0 {
		return nil, actions.NewTblNotExistError(doltdb.ToTableNames(missingTables, doltdb.DefaultSchemaName))
	} else if len(missingStaged) > 0 {
		return nil, actions.NewTblStagedError(doltdb.ToTableNames(missingStaged, doltdb.DefaultSchemaName))
	}

	return TableNames, nil
}
