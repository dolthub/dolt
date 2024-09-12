// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

// doltVerifyConstraints is the stored procedure version for the CLI command `dolt constraints verify`.
func doltVerifyConstraints(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltConstraintsVerify(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltConstraintsVerify(ctx *sql.Context, args []string) (int, error) {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return 1, err
	}

	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	workingSet, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return 1, err
	}
	workingRoot := workingSet.WorkingRoot()
	headCommit, err := dSess.GetHeadCommit(ctx, dbName)
	if err != nil {
		return 1, err
	}

	apr, err := cli.CreateVerifyConstraintsArgParser("doltVerifyConstraints").Parse(args)
	if err != nil {
		return 1, err
	}

	verifyAll := apr.Contains(cli.AllFlag)
	outputOnly := apr.Contains(cli.OutputOnlyFlag)

	var comparingRoot doltdb.RootValue
	if verifyAll {
		comparingRoot, err = doltdb.EmptyRootValue(ctx, workingRoot.VRW(), workingRoot.NodeStore())
		if err != nil {
			return 1, err
		}
	} else {
		comparingRoot, err = headCommit.GetRootValue(ctx)
		if err != nil {
			return 1, err
		}
	}

	tableSet, err := parseTablesToCheck(ctx, workingRoot, apr)
	if err != nil {
		return 1, err
	}

	// Check for all non-FK constraint violations
	newRoot, tablesWithViolations, err := calculateViolations(ctx, workingRoot, comparingRoot, tableSet)
	if err != nil {
		return 1, err
	}

	if !outputOnly {
		err = dSess.SetWorkingRoot(ctx, dbName, newRoot)
		if err != nil {
			return 1, err
		}
	}

	if tablesWithViolations.Size() == 0 {
		// no violations were found
		return 0, nil
	}

	// TODO: We only return 1 or 0 to indicate if there were any constraint violations or not. This isn't
	//       super useful to customers, and not how the CLI command works. It would be better to return
	//       results that indicate the total number of violations found for the specified tables, and
	//       potentially also a human readable message.
	return 1, nil
}

// calculateViolations calculates all constraint violations between |workingRoot| and |comparingRoot| for the
// tables in |tableSet|. Returns the new root with the violations, and a set of table names that have violations.
// Note that constraint violations detected for ALL existing tables will be stored in the dolt_constraint_violations
// tables, but the returned set of table names will be a subset of |tableSet|.
func calculateViolations(ctx *sql.Context, workingRoot, comparingRoot doltdb.RootValue, tableSet *doltdb.TableNameSet) (doltdb.RootValue, *doltdb.TableNameSet, error) {
	var recordViolationsForTables map[doltdb.TableName]struct{} = nil
	if tableSet.Size() > 0 {
		recordViolationsForTables = make(map[doltdb.TableName]struct{})
		for _, table := range tableSet.AsSlice() {
			recordViolationsForTables[table.ToLower()] = struct{}{}
		}
	}

	mergeOpts := merge.MergeOpts{
		IsCherryPick:              false,
		KeepSchemaConflicts:       true,
		ReverifyAllConstraints:    true,
		RecordViolationsForTables: recordViolationsForTables,
	}
	mergeResults, err := merge.MergeRoots(ctx, comparingRoot, workingRoot, comparingRoot, workingRoot, comparingRoot,
		editor.Options{}, mergeOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("error calculating constraint violations: %w", err)
	}

	tablesWithViolations := doltdb.NewTableNameSet(nil)
	for _, tableName := range tableSet.AsSlice() {
		table, ok, err := mergeResults.Root.GetTable(ctx, tableName)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("table %s not found", tableName)
		}
		artifacts, err := table.GetArtifacts(ctx)
		if err != nil {
			return nil, nil, err
		}
		constraintViolationCount, err := artifacts.ConstraintViolationCount(ctx)
		if err != nil {
			return nil, nil, err
		}
		if constraintViolationCount > 0 {
			tablesWithViolations.Add(tableName)
		}
	}

	return mergeResults.Root, tablesWithViolations, nil
}

// parseTablesToCheck returns a set of table names to check for constraint violations. If no tables are specified, then
// all tables in the root are returned.
func parseTablesToCheck(ctx *sql.Context, workingRoot doltdb.RootValue, apr *argparser.ArgParseResults) (*doltdb.TableNameSet, error) {
	tableSet := doltdb.NewTableNameSet(nil)
	for _, val := range apr.Args {
		tableName, _, ok, err := resolve.Table(ctx, workingRoot, val)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, sql.ErrTableNotFound.New(val)
		}

		tableSet.Add(tableName)
	}

	// If no tables were explicitly specified, then check all tables
	if tableSet.Size() == 0 {
		// TODO: schema search path
		names, err := workingRoot.GetTableNames(ctx, doltdb.DefaultSchemaName)
		if err != nil {
			return nil, err
		}
		tableSet.Add(doltdb.ToTableNames(names, doltdb.DefaultSchemaName)...)
	}

	return tableSet, nil
}
