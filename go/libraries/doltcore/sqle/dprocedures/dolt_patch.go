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
	"sort"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

// doltPatch is the stored procedure version for the CLI command `dolt patch` (CLI command not implemented yet).
func doltPatch(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltPatch(ctx, args)
	if err != nil {
		return nil, err
	}
	return newPatchRowIter(res), nil
}

func doDoltPatch(ctx *sql.Context, args []string) ([]string, error) {
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return nil, fmt.Errorf("error: empty database name")
	}

	apr, err := cli.CreatePatchArgParser().Parse(args)
	if err != nil {
		return nil, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	doltDB, ok := dSess.GetDoltDB(ctx, dbName)
	if !ok {
		return nil, fmt.Errorf("failed to get DoltDB")
	}

	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return nil, fmt.Errorf("failed to get dbData")
	}
	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	fromRef, fromRoot, toRef, toRoot, tables := parseRevisionsAndTablesArgs(ctx, dbData, doltDB, roots, apr)
	tableSet, err := validateTablesAndGetTablesSet(ctx, fromRoot, toRoot, tables)
	if err != nil {
		return nil, err
	}

	tableDeltas, err := diff.GetTableDeltas(ctx, fromRoot, toRoot)
	if err != nil {
		return nil, errhand.BuildDError("error: unable to diff tables").AddCause(err).Build()
	}

	sort.Slice(tableDeltas, func(i, j int) bool {
		return strings.Compare(tableDeltas[i].ToName, tableDeltas[j].ToName) < 0
	})

	var finalRes []string
	for _, td := range tableDeltas {
		if !tableSet.Contains(td.FromName) && !tableSet.Contains(td.ToName) {
			continue
		}
		if td.FromTable == nil && td.ToTable == nil {
			return nil, errhand.BuildDError("error: both tables in tableDelta are nil").Build()
		}

		ddlStatements, err := getSchemaSqlPatch(ctx, toRoot, td)
		if err != nil {
			return nil, err
		}
		finalRes = append(finalRes, ddlStatements...)

		if canGetDataDiff(ctx, td) {
			res, err := getUserTableSqlPatch(ctx, dbData, td, fromRef, toRef)
			if err != nil {
				return nil, err
			}
			finalRes = append(finalRes, res...)
		}
	}

	return finalRes, nil
}

func getSchemaSqlPatch(ctx *sql.Context, toRoot *doltdb.RootValue, td diff.TableDelta) ([]string, error) {
	toSchemas, err := toRoot.GetAllSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not read schemas from toRoot, cause: %s", err.Error())
	}

	return diff.SqlSchemaDiff(ctx, td, toSchemas)
}

func canGetDataDiff(ctx *sql.Context, td diff.TableDelta) bool {
	if td.IsDrop() {
		return false // don't output DELETE FROM statements after DROP TABLE
	}
	// not diffable
	if !schema.ArePrimaryKeySetsDiffable(td.Format(), td.FromSch, td.ToSch) {
		ctx.Session.Warn(&sql.Warning{
			Level:   "Warning",
			Code:    mysql.ERNotSupportedYet,
			Message: fmt.Sprintf("Primary key sets differ between revisions for table '%s', skipping data diff", td.ToName),
		})
		return false
	}
	// cannot sql diff
	if td.ToSch == nil || (td.FromSch != nil && !schema.SchemasAreEqual(td.FromSch, td.ToSch)) {
		// TODO(8/24/22 Zach): this is overly broad, we can absolutely do better
		ctx.Session.Warn(&sql.Warning{
			Level:   "Warning",
			Code:    mysql.ERNotSupportedYet,
			Message: fmt.Sprintf("Incompatible schema change, skipping data diff for table '%s'", td.ToName),
		})
		return false
	}
	return true
}

func getUserTableSqlPatch(ctx *sql.Context, dbData env.DbData, td diff.TableDelta, fromRef, toRef string) ([]string, error) {
	// ToTable is used as target table as cannot be nil at this point
	diffSch, projections, ri, err := getDiffQuery(ctx, dbData, td, fromRef, toRef)
	if err != nil {
		return nil, err
	}

	targetPkSch, err := sqlutil.FromDoltSchema(td.ToName, td.ToSch)
	if err != nil {
		return nil, err
	}

	return getDiffResults(ctx, diffSch, targetPkSch.Schema, projections, ri, td.ToName, td.ToSch)
}

// getDiffQuery returns diff schema for specified columns and array of sql.Expression as projection to be used
// on diff table function row iter. This function attempts to imitate running a query
// fmt.Sprintf("select %s, %s from dolt_diff('%s', '%s', '%s')", columnsWithDiff, "diff_type", fromRef, toRef, tableName)
// on sql engine, which returns the schema and rowIter of the final data diff result.
func getDiffQuery(ctx *sql.Context, dbData env.DbData, td diff.TableDelta, fromRef, toRef string) (sql.Schema, []sql.Expression, sql.RowIter, error) {
	diffTableSchema, j, err := dtables.GetDiffTableSchemaAndJoiner(td.ToTable.Format(), td.FromSch, td.ToSch)
	if err != nil {
		return nil, nil, nil, err
	}
	diffPKSch, err := sqlutil.FromDoltSchema("", diffTableSchema)
	if err != nil {
		return nil, nil, nil, err
	}

	columnsWithDiff := getColumnNamesWithDiff(td.FromSch, td.ToSch)
	diffSqlSch, projections := getDiffSqlSchema(diffPKSch.Schema, columnsWithDiff)

	// using arbitrary time since we do not care about the commit time in the result
	now := time.Now()
	dp := dtables.NewDiffPartition(td.ToTable, td.FromTable, toRef, fromRef, (*types.Timestamp)(&now), (*types.Timestamp)(&now), td.ToSch, td.FromSch)
	ri := dtables.NewDiffPartitionRowIter(*dp, dbData.Ddb, j)

	return diffSqlSch, projections, ri, nil
}

func getColumnNamesWithDiff(fromSch, toSch schema.Schema) []string {
	var cols []string

	if fromSch != nil {
		_ = fromSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			cols = append(cols, fmt.Sprintf("from_%s", col.Name))
			return false, nil
		})
	}
	if toSch != nil {
		_ = toSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			cols = append(cols, fmt.Sprintf("to_%s", col.Name))
			return false, nil
		})
	}
	return cols
}

// getDiffSqlSchema returns the schema of columns with data diff and "diff_type". This is used for diff splitter.
// When extracting the diff schema, the ordering must follow the ordering of given columns
func getDiffSqlSchema(diffTableSch sql.Schema, columns []string) (sql.Schema, []sql.Expression) {
	type column struct {
		sqlCol *sql.Column
		idx    int
	}

	columns = append(columns, "diff_type")
	colMap := make(map[string]*column)
	for _, c := range columns {
		colMap[c] = nil
	}

	var cols = make([]*sql.Column, len(columns))
	var getFieldCols = make([]sql.Expression, len(columns))

	for i, c := range diffTableSch {
		if _, ok := colMap[c.Name]; ok {
			colMap[c.Name] = &column{c, i}
		}
	}

	for i, c := range columns {
		col := colMap[c].sqlCol
		cols[i] = col
		getFieldCols[i] = expression.NewGetField(colMap[c].idx, col.Type, col.Name, col.Nullable)
	}

	return cols, getFieldCols
}

func getDiffResults(ctx *sql.Context, diffQuerySch, targetSch sql.Schema, projections []sql.Expression, iter sql.RowIter, tn string, tsch schema.Schema) ([]string, error) {
	ds, err := diff.NewDiffSplitter(diffQuerySch, targetSch)
	if err != nil {
		return nil, err
	}

	var res []string
	for {
		r, err := iter.Next(ctx)
		if err == io.EOF {
			return res, nil
		} else if err != nil {
			return nil, err
		}

		r, err = plan.ProjectRow(ctx, projections, r)
		if err != nil {
			return nil, err
		}

		oldRow, newRow, err := ds.SplitDiffResultRow(r)
		if err != nil {
			return nil, err
		}

		var stmt string
		if oldRow.Row != nil {
			stmt, err = diff.GetDataDiffStatement(tn, tsch, oldRow.Row, oldRow.RowDiff, oldRow.ColDiffs)
			if err != nil {
				return nil, err
			}
		}

		if newRow.Row != nil {
			stmt, err = diff.GetDataDiffStatement(tn, tsch, newRow.Row, newRow.RowDiff, newRow.ColDiffs)
			if err != nil {
				return nil, err
			}
		}

		if stmt != "" {
			res = append(res, stmt)
		}
	}
}

// parseRevisionsAndTablesArgs checks given arguments whether each refers to a revision or a table name.
// It returns from revision name, from root values, to revision name, to root values and potential table names.
func parseRevisionsAndTablesArgs(ctx *sql.Context, dbData env.DbData, doltDB *doltdb.DoltDB, roots doltdb.Roots, apr *argparser.ArgParseResults) (string, *doltdb.RootValue, string, *doltdb.RootValue, []string) {
	var fromRef, toRef string
	var fromRoot, toRoot *doltdb.RootValue

	fromRoot = roots.Staged
	fromRef = "STAGED"
	toRoot = roots.Working
	toRef = "WORKING"
	if apr.Contains(cli.CachedFlag) {
		fromRoot = roots.Head
		fromRef = "HEAD"
		toRoot = roots.Staged
		toRef = "STAGED"
	}

	// `dolt diff`
	if apr.NArg() == 0 {
		return fromRef, fromRoot, toRef, toRoot, apr.Args
	}

	from, ok := diff.MaybeResolveRoot(ctx, dbData.Rsr, doltDB, apr.Args[0])
	if !ok {
		// `dolt diff [...tables]`
		return fromRef, fromRoot, toRef, toRoot, apr.Args
	}

	fromRoot = from
	fromRef = apr.Args[0]

	if apr.NArg() == 1 {
		// `dolt diff from_commit`
		return fromRef, fromRoot, toRef, toRoot, apr.Args[1:]
	}

	to, ok := diff.MaybeResolveRoot(ctx, dbData.Rsr, doltDB, apr.Args[1])
	if !ok {
		// `dolt diff from_commit [...tables]`
		return fromRef, fromRoot, toRef, toRoot, apr.Args[1:]
	}

	toRoot = to
	toRef = apr.Args[1]

	// `dolt diff from_commit to_commit [...tables]`
	return fromRef, fromRoot, toRef, toRoot, apr.Args[2:]
}

// validateTablesAndGetTablesSet takes array of table names or an empty array and returns the table names
// in string set type. If the array is empty, it returns union of table names on from and to roots.
func validateTablesAndGetTablesSet(ctx context.Context, fromRoot, toRoot *doltdb.RootValue, tables []string) (*set.StrSet, error) {
	tableSet := set.NewStrSet(nil)

	// if no tables or docs were specified as args, diff all tables and docs
	if len(tables) == 0 {
		utn, err := doltdb.UnionTableNames(ctx, fromRoot, toRoot)
		if err != nil {
			return nil, err
		}
		tableSet.Add(utn...)
	} else {
		for _, tableName := range tables {
			// verify table args exist in at least one root
			_, ok, err := fromRoot.GetTable(ctx, tableName)
			if err != nil {
				return nil, err
			}
			if ok {
				tableSet.Add(tableName)
				continue
			}

			_, ok, err = toRoot.GetTable(ctx, tableName)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("table %s does not exist in either revision", tableName)
			}
		}
	}

	return tableSet, nil
}

var _ sql.RowIter = (*patchRowIter)(nil)

type patchRowIter struct {
	stmts []string
	idx   int
}

func newPatchRowIter(stmts []string) sql.RowIter {
	return &patchRowIter{
		stmts: stmts,
		idx:   0,
	}
}

func (p *patchRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	defer func() {
		p.idx++
	}()

	if p.idx >= len(p.stmts) {
		return nil, io.EOF
	}

	if p.stmts == nil {
		return nil, io.EOF
	}

	stmt := p.stmts[p.idx]
	return sql.Row{stmt}, nil
}

func (p *patchRowIter) Close(_ *sql.Context) error {
	p.stmts = nil
	p.idx = 0
	return nil
}
