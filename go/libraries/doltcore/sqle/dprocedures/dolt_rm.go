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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
)

// doltRm is the stored procedure for the cli command dolt rm
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
		return 1, fmt.Errorf("Could not load roots for database %s", dbName)
	}

	checkStaged := apr.Contains(cli.CachedFlag)
	verifiedTables, err := verifyTables(ctx, apr.Args, checkStaged, roots)
	if err != nil {
		return 1, err
	}

	staged, err := roots.Staged.RemoveTables(ctx, false, false, verifiedTables...)
	if err != nil {
		return 1, err
	}
	roots.Staged = staged

	// The staged root check above does not validate FK constraints that only exist in
	// the working set, so the working set must also be checked.
	if !checkStaged {
		working, err := roots.Working.RemoveTables(ctx, false, false, verifiedTables...)
		if err != nil {
			return 1, err
		}
		roots.Working = working
	} else {
		// With |--cached|, a table may already be absent from working (for example
		// after a DROP TABLE), so only tables present in working are passed to the FK check.
		var workingTables []doltdb.TableName
		for _, tbl := range verifiedTables {
			ok, err := roots.Working.HasTable(ctx, tbl)
			if err != nil {
				return 1, err
			}
			if ok {
				workingTables = append(workingTables, tbl)
			}
		}
		if _, err = roots.Working.RemoveTables(ctx, false, false, workingTables...); err != nil {
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
	var missingStagedTables []string
	var tableNames []doltdb.TableName

	var unstagedTables []string

	for _, name := range unqualifiedTables {
		_, okHead, err := resolve.TableName(ctx, roots.Head, name)
		if err != nil {
			return nil, err
		}
		tblName, okStage, err := resolve.TableName(ctx, roots.Staged, name)
		if err != nil {
			return nil, err
		}

		// With |--cached|, the working set is not modified, so unstaged working changes are not a problem.
		if !checkStaged {
			hasChanges, err := hasUnstagedChanges(ctx, roots, name, okStage, okHead)
			if err != nil {
				return nil, err
			}
			if hasChanges {
				unstagedTables = append(unstagedTables, name)
				continue
			}
		}

		// If the table exists in staged:
		// If we use --cached, or it exists in HEAD, we can remove it safely
		// Otherwise we error out.
		// If the table does not exist in staged:
		// If it is in HEAD we can remove it safely
		// If it isn't in HEAD it doesn't exist, and so we error
		if okStage {
			if okHead || checkStaged {
				tableNames = append(tableNames, tblName)
			} else {
				missingStagedTables = append(missingStagedTables, name)
			}
		} else {
			if okHead {
				tableNames = append(tableNames, tblName)
			} else {
				missingTables = append(missingTables, name)
			}
		}
	}

	if len(missingTables) > 0 {
		return nil, actions.NewTblNotExistError(doltdb.ToTableNames(missingTables, doltdb.DefaultSchemaName))
	} else if !checkStaged && len(unstagedTables) > 0 {
		return nil, actions.NewTblUnstagedError(doltdb.ToTableNames(unstagedTables, doltdb.DefaultSchemaName))
	} else if len(missingStagedTables) > 0 {
		return nil, actions.NewTblStagedError(doltdb.ToTableNames(missingStagedTables, doltdb.DefaultSchemaName))
	}

	return tableNames, nil
}

// hasUnstagedChanges checks the HEAD and staged roots for the table, then compares them against the table in the working root.
// inStaged and inHead are booleans identifying if the table exists in the staged root, or the head root, respectively.
func hasUnstagedChanges(ctx *sql.Context, roots doltdb.Roots, name string, inStaged bool, inHead bool) (bool, error) {
	checkTableChanges := func(rootVal doltdb.RootValue) (bool, error) {
		tableDiff, err := diff.GetTableDeltas(ctx, rootVal, roots.Working)
		if err != nil {
			return false, err
		}

		for _, tbl := range tableDiff {
			hasChanges, err := tbl.HasChanges()
			if err != nil {
				return false, err
			}
			if tbl.ToName.String() == name && hasChanges {
				return true, nil
			}
		}
		return false, nil
	}

	// Check diff between working and staged.
	// We'll check this if the table exists in the staged root or if it doesn't exist in the HEAD root
	if inStaged || !inHead {
		hasChanges, err := checkTableChanges(roots.Staged)
		if err != nil {
			return false, err
		}
		if hasChanges {
			return true, nil
		}
	}

	// Now check diff between working and HEAD
	// We'll check this if the table exists in the HEAD root or if it doesn't exist in the staged root
	if inHead || !inStaged {
		hasChanges, err := checkTableChanges(roots.Head)
		if err != nil {
			return false, err
		}
		if hasChanges {
			return true, nil
		}
	}

	return false, nil
}
