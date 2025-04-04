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

package dtablefunctions

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

var _ sql.TableFunction = (*PatchTableFunction)(nil)
var _ sql.ExecSourceRel = (*PatchTableFunction)(nil)
var _ sql.IndexAddressable = (*PatchTableFunction)(nil)
var _ sql.IndexedTable = (*PatchTableFunction)(nil)
var _ sql.TableNode = (*PatchTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*PatchTableFunction)(nil)

const (
	diffTypeSchema = "schema"
	diffTypeData   = "data"
)

var schemaChangePartitionKey = []byte(diffTypeSchema)
var dataChangePartitionKey = []byte(diffTypeData)
var schemaAndDataChangePartitionKey = []byte("all")

const (
	orderColumnName           = "statement_order"
	fromColumnName            = "from_commit_hash"
	toColumnName              = "to_commit_hash"
	tableNameColumnName       = "table_name"
	diffTypeColumnName        = "diff_type"
	statementColumnName       = "statement"
	patchTableDefaultRowCount = 100
)

type PatchTableFunction struct {
	ctx *sql.Context

	fromCommitExpr sql.Expression
	toCommitExpr   sql.Expression
	dotCommitExpr  sql.Expression
	tableNameExpr  sql.Expression
	database       sql.Database
}

func (p *PatchTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(p.Schema())
	numRows, _, err := p.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (p *PatchTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return patchTableDefaultRowCount, false, nil
}

func (p *PatchTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

type Partition struct {
	key []byte
}

func (p *Partition) Key() []byte { return p.key }

// UnderlyingTable implements the plan.TableNode interface
func (p *PatchTableFunction) UnderlyingTable() sql.Table {
	return p
}

// Collation implements the sql.Table interface.
func (p *PatchTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data. This data has a single partition.
func (p *PatchTableFunction) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return dtables.NewSliceOfPartitionsItr([]sql.Partition{
		&Partition{key: schemaAndDataChangePartitionKey},
	}), nil
}

// PartitionRows is a sql.Table interface function that takes a partition and returns all rows in that partition.
// This table has a partition for just schema changes, one for just data changes, and one for both.
// TODO: schema names
func (p *PatchTableFunction) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	fromCommitVal, toCommitVal, dotCommitVal, tableName, err := p.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := p.database.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unable to get dolt database")
	}

	fromRefDetails, toRefDetails, err := loadDetailsForRefs(ctx, fromCommitVal, toCommitVal, dotCommitVal, sqledb)
	if err != nil {
		return nil, err
	}

	tableDeltas, err := diff.GetTableDeltas(ctx, fromRefDetails.root, toRefDetails.root)
	if err != nil {
		return nil, err
	}

	sort.Slice(tableDeltas, func(i, j int) bool {
		return tableDeltas[i].ToName.Less(tableDeltas[j].ToName)
	})

	// If tableNameExpr defined, return a single table patch result
	if p.tableNameExpr != nil {
		delta := findMatchingDelta(tableDeltas, tableName)
		if delta.FromTable == nil && delta.ToTable == nil {
			_, _, fromTblExists, err := resolve.Table(ctx, fromRefDetails.root, tableName)
			if err != nil {
				return nil, err
			}
			_, _, toTblExists, err := resolve.Table(ctx, toRefDetails.root, tableName)
			if err != nil {
				return nil, err
			}
			if !fromTblExists && !toTblExists {
				return nil, sql.ErrTableNotFound.New(tableName)
			}
		}

		tableDeltas = []diff.TableDelta{delta}
	}

	includeSchemaDiff := bytes.Equal(partition.Key(), schemaAndDataChangePartitionKey) || bytes.Equal(partition.Key(), schemaChangePartitionKey)
	includeDataDiff := bytes.Equal(partition.Key(), schemaAndDataChangePartitionKey) || bytes.Equal(partition.Key(), dataChangePartitionKey)

	patches, err := getPatchNodes(ctx, sqledb.DbData(), tableDeltas, fromRefDetails, toRefDetails, includeSchemaDiff, includeDataDiff)
	if err != nil {
		return nil, err
	}

	return newPatchTableFunctionRowIter(patches, fromRefDetails.hashStr, toRefDetails.hashStr), nil
}

// LookupPartitions is a sql.IndexedTable interface function that takes an index lookup and returns the set of corresponding partitions.
func (p *PatchTableFunction) LookupPartitions(context *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if lookup.Index.ID() == diffTypeColumnName {
		diffTypes, ok := index.LookupToPointSelectStr(lookup)
		if !ok {
			return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(lookup.Ranges))
		}

		includeSchemaDiff := slices.Contains(diffTypes, diffTypeSchema)
		includeDataDiff := slices.Contains(diffTypes, diffTypeData)

		if includeSchemaDiff && includeDataDiff {
			return dtables.NewSliceOfPartitionsItr([]sql.Partition{
				&Partition{key: schemaAndDataChangePartitionKey},
			}), nil
		}

		if includeSchemaDiff {
			return dtables.NewSliceOfPartitionsItr([]sql.Partition{
				&Partition{key: schemaChangePartitionKey},
			}), nil
		}

		if includeDataDiff {
			return dtables.NewSliceOfPartitionsItr([]sql.Partition{
				&Partition{key: dataChangePartitionKey},
			}), nil
		}

		return dtables.NewSliceOfPartitionsItr([]sql.Partition{}), nil
	}

	return dtables.NewSliceOfPartitionsItr([]sql.Partition{
		&Partition{key: schemaAndDataChangePartitionKey},
	}), nil
}

func (p *PatchTableFunction) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return p
}

func (p *PatchTableFunction) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return []sql.Index{
		index.MockIndex(p.database.Name(), p.Name(), diffTypeColumnName, types.StringKind, false),
	}, nil
}

func (p *PatchTableFunction) PreciseMatch() bool {
	return true
}

var patchTableSchema = sql.Schema{
	&sql.Column{Name: orderColumnName, Type: sqltypes.Uint64, PrimaryKey: true, Nullable: false},
	&sql.Column{Name: fromColumnName, Type: sqltypes.LongText, Nullable: false},
	&sql.Column{Name: toColumnName, Type: sqltypes.LongText, Nullable: false},
	&sql.Column{Name: tableNameColumnName, Type: sqltypes.LongText, Nullable: false},
	&sql.Column{Name: diffTypeColumnName, Type: sqltypes.LongText, Nullable: false},
	&sql.Column{Name: statementColumnName, Type: sqltypes.LongText, Nullable: false},
}

// NewInstance creates a new instance of TableFunction interface
func (p *PatchTableFunction) NewInstance(ctx *sql.Context, db sql.Database, exprs []sql.Expression) (sql.Node, error) {
	newInstance := &PatchTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(exprs...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Resolved implements the sql.Resolvable interface
func (p *PatchTableFunction) Resolved() bool {
	if p.tableNameExpr != nil {
		return p.commitsResolved() && p.tableNameExpr.Resolved()
	}
	return p.commitsResolved()
}

func (p *PatchTableFunction) IsReadOnly() bool {
	return true
}

func (p *PatchTableFunction) commitsResolved() bool {
	if p.dotCommitExpr != nil {
		return p.dotCommitExpr.Resolved()
	}
	return p.fromCommitExpr.Resolved() && p.toCommitExpr.Resolved()
}

// String implements the Stringer interface
func (p *PatchTableFunction) String() string {
	if p.dotCommitExpr != nil {
		if p.tableNameExpr != nil {
			return fmt.Sprintf("DOLT_PATCH(%s, %s)", p.dotCommitExpr.String(), p.tableNameExpr.String())
		}
		return fmt.Sprintf("DOLT_PATCH(%s)", p.dotCommitExpr.String())
	}
	if p.tableNameExpr != nil {
		return fmt.Sprintf("DOLT_PATCH(%s, %s, %s)", p.fromCommitExpr.String(), p.toCommitExpr.String(), p.tableNameExpr.String())
	}
	if p.fromCommitExpr != nil && p.toCommitExpr != nil {
		return fmt.Sprintf("DOLT_PATCH(%s, %s)", p.fromCommitExpr.String(), p.toCommitExpr.String())
	}
	return fmt.Sprintf("DOLT_PATCH(<INVALID>)")
}

// Schema implements the sql.Node interface.
func (p *PatchTableFunction) Schema() sql.Schema {
	return patchTableSchema
}

// Children implements the sql.Node interface.
func (p *PatchTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (p *PatchTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return p, nil
}

// CheckAuth implements the interface sql.AuthorizationCheckerNode.
func (p *PatchTableFunction) CheckAuth(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if p.tableNameExpr != nil {
		if !sqltypes.IsText(p.tableNameExpr.Type()) {
			return ExpressionIsDeferred(p.tableNameExpr)
		}

		tableNameVal, err := p.tableNameExpr.Eval(p.ctx, nil)
		if err != nil {
			return false
		}
		tableName, ok := tableNameVal.(string)
		if !ok {
			return false
		}

		subject := sql.PrivilegeCheckSubject{Database: p.database.Name(), Table: tableName}
		return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
	}

	tblNames, err := p.database.GetTableNames(ctx)
	if err != nil {
		return false
	}

	operations := make([]sql.PrivilegedOperation, 0, len(tblNames))
	for _, tblName := range tblNames {
		subject := sql.PrivilegeCheckSubject{Database: p.database.Name(), Table: tblName}
		operations = append(operations, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
	}

	return opChecker.UserHasPrivileges(ctx, operations...)
}

// Expressions implements the sql.Expressioner interface.
func (p *PatchTableFunction) Expressions() []sql.Expression {
	exprs := []sql.Expression{}
	if p.dotCommitExpr != nil {
		exprs = append(exprs, p.dotCommitExpr)
	} else {
		exprs = append(exprs, p.fromCommitExpr, p.toCommitExpr)
	}
	if p.tableNameExpr != nil {
		exprs = append(exprs, p.tableNameExpr)
	}
	return exprs
}

// WithExpressions implements the sql.Expressioner interface.
func (p *PatchTableFunction) WithExpressions(expr ...sql.Expression) (sql.Node, error) {
	if len(expr) < 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(p.Name(), "1 to 3", len(expr))
	}

	for _, expr := range expr {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(p.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(p.Name(), expr.String())
		}
	}

	newPtf := *p
	if strings.Contains(expr[0].String(), "..") {
		if len(expr) < 1 || len(expr) > 2 {
			return nil, sql.ErrInvalidArgumentNumber.New(newPtf.Name(), "1 or 2", len(expr))
		}
		newPtf.dotCommitExpr = expr[0]
		if len(expr) == 2 {
			newPtf.tableNameExpr = expr[1]
		}
	} else {
		if len(expr) < 2 || len(expr) > 3 {
			return nil, sql.ErrInvalidArgumentNumber.New(newPtf.Name(), "2 or 3", len(expr))
		}
		newPtf.fromCommitExpr = expr[0]
		newPtf.toCommitExpr = expr[1]
		if len(expr) == 3 {
			newPtf.tableNameExpr = expr[2]
		}
	}

	// validate the expressions
	if newPtf.dotCommitExpr != nil {
		if !sqltypes.IsText(newPtf.dotCommitExpr.Type()) && !expression.IsBindVar(newPtf.dotCommitExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newPtf.Name(), newPtf.dotCommitExpr.String())
		}
	} else {
		if !sqltypes.IsText(newPtf.fromCommitExpr.Type()) && !expression.IsBindVar(newPtf.fromCommitExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newPtf.Name(), newPtf.fromCommitExpr.String())
		}
		if !sqltypes.IsText(newPtf.toCommitExpr.Type()) && !expression.IsBindVar(newPtf.toCommitExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newPtf.Name(), newPtf.toCommitExpr.String())
		}
	}

	if newPtf.tableNameExpr != nil {
		if !sqltypes.IsText(newPtf.tableNameExpr.Type()) && !expression.IsBindVar(newPtf.tableNameExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newPtf.Name(), newPtf.tableNameExpr.String())
		}
	}

	return &newPtf, nil
}

// Database implements the sql.Databaser interface
func (p *PatchTableFunction) Database() sql.Database {
	return p.database
}

// WithDatabase implements the sql.Databaser interface
func (p *PatchTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	np := *p
	np.database = database
	return &np, nil
}

// Name implements the sql.TableFunction interface
func (p *PatchTableFunction) Name() string {
	return "dolt_patch"
}

// RowIter implements the sql.ExecSourceRel interface
func (p *PatchTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	partitions, err := p.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewTableRowIter(ctx, p, partitions), nil
}

// evaluateArguments returns fromCommitVal, toCommitVal, dotCommitVal, and tableName.
// It evaluates the argument expressions to turn them into values this PatchTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (p *PatchTableFunction) evaluateArguments() (interface{}, interface{}, interface{}, string, error) {
	var tableName string
	if p.tableNameExpr != nil {
		tableNameVal, err := p.tableNameExpr.Eval(p.ctx, nil)
		if err != nil {
			return nil, nil, nil, "", err
		}
		tn, ok := tableNameVal.(string)
		if !ok {
			return nil, nil, nil, "", ErrInvalidTableName.New(p.tableNameExpr.String())
		}
		tableName = tn
	}

	if p.dotCommitExpr != nil {
		dotCommitVal, err := p.dotCommitExpr.Eval(p.ctx, nil)
		if err != nil {
			return nil, nil, nil, "", err
		}

		return nil, nil, dotCommitVal, tableName, nil
	}

	fromCommitVal, err := p.fromCommitExpr.Eval(p.ctx, nil)
	if err != nil {
		return nil, nil, nil, "", err
	}

	toCommitVal, err := p.toCommitExpr.Eval(p.ctx, nil)
	if err != nil {
		return nil, nil, nil, "", err
	}

	return fromCommitVal, toCommitVal, nil, tableName, nil
}

type patchNode struct {
	tblName          doltdb.TableName
	schemaPatchStmts []string
	dataPatchStmts   []string
}

func getPatchNodes(ctx *sql.Context, dbData env.DbData, tableDeltas []diff.TableDelta, fromRefDetails, toRefDetails *refDetails, includeSchemaDiff, includeDataDiff bool) (patches []*patchNode, err error) {
	for _, td := range tableDeltas {
		if td.FromTable == nil && td.ToTable == nil {
			// no diff
			if !strings.HasPrefix(td.FromName.Name, diff.DBPrefix) || !strings.HasPrefix(td.ToName.Name, diff.DBPrefix) {
				continue
			}

			// db collation diff
			dbName := strings.TrimPrefix(td.ToName.Name, diff.DBPrefix)
			fromColl, cerr := fromRefDetails.root.GetCollation(ctx)
			if cerr != nil {
				return nil, cerr
			}
			toColl, cerr := toRefDetails.root.GetCollation(ctx)
			if cerr != nil {
				return nil, cerr
			}
			alterDBCollStmt := sqlfmt.AlterDatabaseCollateStmt(dbName, fromColl, toColl)
			patches = append(patches, &patchNode{
				tblName:          td.FromName,
				schemaPatchStmts: []string{alterDBCollStmt},
				dataPatchStmts:   []string{},
			})
		}

		tblName := td.ToName
		if td.IsDrop() {
			tblName = td.FromName
		}

		// Get SCHEMA DIFF
		var schemaStmts []string
		if includeSchemaDiff {
			schemaStmts, err = sqlfmt.GenerateSqlPatchSchemaStatements(ctx, toRefDetails.root, td)
			if err != nil {
				return nil, err
			}
		}

		// Get DATA DIFF
		var dataStmts []string
		if includeDataDiff && canGetDataDiff(ctx, td) {
			dataStmts, err = getUserTableDataSqlPatch(ctx, dbData, td, fromRefDetails, toRefDetails)
			if err != nil {
				return nil, err
			}
		}

		patches = append(patches, &patchNode{tblName: tblName, schemaPatchStmts: schemaStmts, dataPatchStmts: dataStmts})
	}

	return patches, nil
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

	return true
}

func getUserTableDataSqlPatch(ctx *sql.Context, dbData env.DbData, td diff.TableDelta, fromRefDetails, toRefDetails *refDetails) ([]string, error) {
	// ToTable is used as target table as it cannot be nil at this point
	diffSch, projections, ri, err := getDiffQuery(ctx, dbData, td, fromRefDetails, toRefDetails)
	if err != nil {
		return nil, err
	}

	targetPkSch, err := sqlutil.FromDoltSchema("", td.ToName.Name, td.ToSch)
	if err != nil {
		return nil, err
	}

	return getDataSqlPatchResults(ctx, diffSch, targetPkSch.Schema, projections, ri, td.ToName.Name, td.ToSch)
}

func getDataSqlPatchResults(ctx *sql.Context, diffQuerySch, targetSch sql.Schema, projections []sql.Expression, iter sql.RowIter, tn string, tsch schema.Schema) ([]string, error) {
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

		r, err = rowexec.ProjectRow(ctx, projections, r)
		if err != nil {
			return nil, err
		}

		oldRow, newRow, err := ds.SplitDiffResultRow(ctx, r)
		if err != nil {
			return nil, err
		}

		var stmt string
		if oldRow.Row != nil {
			stmt, err = sqlfmt.GenerateDataDiffStatement(ctx, tn, tsch, oldRow.Row, oldRow.RowDiff, oldRow.ColDiffs)
			if err != nil {
				return nil, err
			}
		}

		if newRow.Row != nil {
			stmt, err = sqlfmt.GenerateDataDiffStatement(ctx, tn, tsch, newRow.Row, newRow.RowDiff, newRow.ColDiffs)
			if err != nil {
				return nil, err
			}
		}

		if stmt != "" {
			res = append(res, stmt)
		}
	}
}

// getDiffQuery returns diff schema for specified columns and array of sql.Expression as projection to be used
// on diff table function row iter. This function attempts to imitate running a query
// fmt.Sprintf("select %s, %s from dolt_diff('%s', '%s', '%s')", columnsWithDiff, "diff_type", fromRef, toRef, tableName)
// on sql engine, which returns the schema and rowIter of the final data diff result.
func getDiffQuery(ctx *sql.Context, dbData env.DbData, td diff.TableDelta, fromRefDetails, toRefDetails *refDetails) (sql.Schema, []sql.Expression, sql.RowIter, error) {
	diffTableSchema, j, err := dtables.GetDiffTableSchemaAndJoiner(td.ToTable.Format(), td.FromSch, td.ToSch)
	if err != nil {
		return nil, nil, nil, err
	}
	diffPKSch, err := sqlutil.FromDoltSchema("", "", diffTableSchema)
	if err != nil {
		return nil, nil, nil, err
	}

	columnsWithDiff := getColumnNamesWithDiff(td.FromSch, td.ToSch)
	diffQuerySqlSch, projections := getDiffQuerySqlSchemaAndProjections(diffPKSch.Schema, columnsWithDiff)

	dp := dtables.NewDiffPartition(td.ToTable, td.FromTable, toRefDetails.hashStr, fromRefDetails.hashStr, toRefDetails.commitTime, fromRefDetails.commitTime, td.ToSch, td.FromSch)
	ri := dtables.NewDiffPartitionRowIter(dp, dbData.Ddb, j)

	return diffQuerySqlSch, projections, ri, nil
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

// getDiffQuerySqlSchemaAndProjections returns the schema of columns with data diff and "diff_type". This is used for diff splitter.
// When extracting the diff schema, the ordering must follow the ordering of given columns
func getDiffQuerySqlSchemaAndProjections(diffTableSch sql.Schema, columns []string) (sql.Schema, []sql.Expression) {
	type column struct {
		sqlCol *sql.Column
		idx    int
	}

	columns = append(columns, diffTypeColumnName)
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

//------------------------------------
// patchTableFunctionRowIter
//------------------------------------

var _ sql.RowIter = (*patchTableFunctionRowIter)(nil)

type patchTableFunctionRowIter struct {
	patches        []*patchNode
	patchIdx       int
	statementIdx   int
	fromRef        string
	toRef          string
	currentPatch   *patchNode
	currentRowIter *sql.RowIter
}

// newPatchTableFunctionRowIter iterates over each patch nodes given returning
// each statement in each patch node as a single row including from_commit_hash,
// to_commit_hash and table_name prepended to diff_type and statement for each patch statement.
func newPatchTableFunctionRowIter(patchNodes []*patchNode, fromRef, toRef string) sql.RowIter {
	return &patchTableFunctionRowIter{
		patches:      patchNodes,
		patchIdx:     0,
		statementIdx: 0,
		fromRef:      fromRef,
		toRef:        toRef,
	}
}

func (itr *patchTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if itr.patchIdx >= len(itr.patches) {
			return nil, io.EOF
		}
		if itr.currentPatch == nil {
			itr.currentPatch = itr.patches[itr.patchIdx]
		}
		if itr.currentRowIter == nil {
			ri := newPatchStatementsRowIter(itr.currentPatch.schemaPatchStmts, itr.currentPatch.dataPatchStmts)
			itr.currentRowIter = &ri
		}

		row, err := (*itr.currentRowIter).Next(ctx)
		if err == io.EOF {
			itr.currentPatch = nil
			itr.currentRowIter = nil
			itr.patchIdx++
			continue
		} else if err != nil {
			return nil, err
		} else {
			itr.statementIdx++
			r := sql.Row{
				itr.statementIdx,                  // statement_order
				itr.fromRef,                       // from_commit_hash
				itr.toRef,                         // to_commit_hash
				itr.currentPatch.tblName.String(), // table_name
			}
			return r.Append(row), nil
		}
	}
}

func (itr *patchTableFunctionRowIter) Close(_ *sql.Context) error {
	return nil
}

//------------------------------------
// patchStatementsRowIter
//------------------------------------

var _ sql.RowIter = (*patchStatementsRowIter)(nil)

type patchStatementsRowIter struct {
	stmts  []string
	ddlLen int
	idx    int
}

// newPatchStatementsRowIter iterates over each patch statements returning row of diff_type of either 'schema' or 'data' with the statement.
func newPatchStatementsRowIter(ddlStmts, dataStmts []string) sql.RowIter {
	return &patchStatementsRowIter{
		stmts:  append(ddlStmts, dataStmts...),
		ddlLen: len(ddlStmts),
		idx:    0,
	}
}

func (p *patchStatementsRowIter) Next(ctx *sql.Context) (sql.Row, error) {
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
	diffType := diffTypeSchema
	if p.idx >= p.ddlLen {
		diffType = diffTypeData
	}

	return sql.Row{
		diffType, // diff_type
		stmt,     // statement
	}, nil
}

func (p *patchStatementsRowIter) Close(_ *sql.Context) error {
	return nil
}
