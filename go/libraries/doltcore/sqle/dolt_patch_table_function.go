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

package sqle

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

var _ sql.TableFunction = (*PatchTableFunction)(nil)

type PatchTableFunction struct {
	ctx *sql.Context

	fromCommitExpr sql.Expression
	toCommitExpr   sql.Expression
	dotCommitExpr  sql.Expression
	tableNameExpr  sql.Expression
	database       sql.Database
}

var patchTableSchema = sql.Schema{
	&sql.Column{Name: "statement_order", Type: sqltypes.Uint64, PrimaryKey: true, Nullable: false},
	&sql.Column{Name: "from_commit_hash", Type: sqltypes.LongText, Nullable: false},
	&sql.Column{Name: "to_commit_hash", Type: sqltypes.LongText, Nullable: false},
	&sql.Column{Name: "table_name", Type: sqltypes.LongText, Nullable: false},
	&sql.Column{Name: "diff_type", Type: sqltypes.LongText, Nullable: false},
	&sql.Column{Name: "statement", Type: sqltypes.LongText, Nullable: false},
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
	return fmt.Sprintf("DOLT_PATCH(%s, %s)", p.fromCommitExpr.String(), p.toCommitExpr.String())
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

// CheckPrivileges implements the interface sql.Node.
func (p *PatchTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if p.tableNameExpr != nil {
		if !sqltypes.IsText(p.tableNameExpr.Type()) {
			return false
		}

		tableNameVal, err := p.tableNameExpr.Eval(p.ctx, nil)
		if err != nil {
			return false
		}
		tableName, ok := tableNameVal.(string)
		if !ok {
			return false
		}

		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(p.database.Name(), tableName, "", sql.PrivilegeType_Select))
	}

	tblNames, err := p.database.GetTableNames(ctx)
	if err != nil {
		return false
	}

	var operations []sql.PrivilegedOperation
	for _, tblName := range tblNames {
		operations = append(operations, sql.NewPrivilegedOperation(p.database.Name(), tblName, "", sql.PrivilegeType_Select))
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
func (p *PatchTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) < 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(p.Name(), "1 to 3", len(expression))
	}

	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(p.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(p.Name(), expr.String())
		}
	}

	newPtf := *p
	if strings.Contains(expression[0].String(), "..") {
		if len(expression) < 1 || len(expression) > 2 {
			return nil, sql.ErrInvalidArgumentNumber.New(newPtf.Name(), "1 or 2", len(expression))
		}
		newPtf.dotCommitExpr = expression[0]
		if len(expression) == 2 {
			newPtf.tableNameExpr = expression[1]
		}
	} else {
		if len(expression) < 2 || len(expression) > 3 {
			return nil, sql.ErrInvalidArgumentNumber.New(newPtf.Name(), "2 or 3", len(expression))
		}
		newPtf.fromCommitExpr = expression[0]
		newPtf.toCommitExpr = expression[1]
		if len(expression) == 3 {
			newPtf.tableNameExpr = expression[2]
		}
	}

	// validate the expressions
	if newPtf.dotCommitExpr != nil {
		if !sqltypes.IsText(newPtf.dotCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(newPtf.Name(), newPtf.dotCommitExpr.String())
		}
	} else {
		if !sqltypes.IsText(newPtf.fromCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(newPtf.Name(), newPtf.fromCommitExpr.String())
		}
		if !sqltypes.IsText(newPtf.toCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(newPtf.Name(), newPtf.toCommitExpr.String())
		}
	}

	if newPtf.tableNameExpr != nil {
		if !sqltypes.IsText(newPtf.tableNameExpr.Type()) {
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

// RowIter implements the sql.Node interface
func (p *PatchTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	fromCommitVal, toCommitVal, dotCommitVal, tableName, err := p.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := p.database.(SqlDatabase)
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
		return strings.Compare(tableDeltas[i].ToName, tableDeltas[j].ToName) < 0
	})

	// If tableNameExpr defined, return a single table patch result
	if p.tableNameExpr != nil {
		fromTblExists, err := fromRefDetails.root.HasTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		toTblExists, err := toRefDetails.root.HasTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if !fromTblExists && !toTblExists {
			return nil, sql.ErrTableNotFound.New(tableName)
		}

		delta := findMatchingDelta(tableDeltas, tableName)
		tableDeltas = []diff.TableDelta{delta}
	}

	patches, err := getPatchNodes(ctx, sqledb.DbData(), tableDeltas, fromRefDetails, toRefDetails)
	if err != nil {
		return nil, err
	}

	return newPatchTableFunctionRowIter(patches, fromRefDetails.hashStr, toRefDetails.hashStr), nil
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
	tblName          string
	schemaPatchStmts []string
	dataPatchStmts   []string
}

func getPatchNodes(ctx *sql.Context, dbData env.DbData, tableDeltas []diff.TableDelta, fromRefDetails, toRefDetails *refDetails) ([]*patchNode, error) {
	var patches []*patchNode
	for _, td := range tableDeltas {
		// no diff
		if td.FromTable == nil && td.ToTable == nil {
			continue
		}

		tblName := td.ToName
		if td.IsDrop() {
			tblName = td.FromName
		}

		// Get SCHEMA DIFF
		schemaStmts, err := getSchemaSqlPatch(ctx, toRefDetails.root, td)
		if err != nil {
			return nil, err
		}

		// Get DATA DIFF
		var dataStmts []string
		if canGetDataDiff(ctx, td) {
			dataStmts, err = getUserTableDataSqlPatch(ctx, dbData, td, fromRefDetails, toRefDetails)
			if err != nil {
				return nil, err
			}
		}

		patches = append(patches, &patchNode{tblName: tblName, schemaPatchStmts: schemaStmts, dataPatchStmts: dataStmts})
	}

	return patches, nil
}

func getSchemaSqlPatch(ctx *sql.Context, toRoot *doltdb.RootValue, td diff.TableDelta) ([]string, error) {
	toSchemas, err := toRoot.GetAllSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not read schemas from toRoot, cause: %s", err.Error())
	}

	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot retrieve schema for table %s, cause: %s", td.ToName, err.Error())
	}

	var ddlStatements []string
	if td.IsDrop() {
		ddlStatements = append(ddlStatements, sqlfmt.DropTableStmt(td.FromName))
	} else if td.IsAdd() {
		toPkSch, err := sqlutil.FromDoltSchema(td.ToName, td.ToSch)
		if err != nil {
			return nil, err
		}
		stmt, err := diff.GenerateCreateTableStatement(td.ToName, td.ToSch, toPkSch, td.ToFks, td.ToFksParentSch)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
		ddlStatements = append(ddlStatements, stmt)
	} else {
		stmts, err := diff.GetNonCreateNonDropTableSqlSchemaDiff(td, toSchemas, fromSch, toSch)
		if err != nil {
			return nil, err
		}
		ddlStatements = append(ddlStatements, stmts...)
	}

	return ddlStatements, nil
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

func getUserTableDataSqlPatch(ctx *sql.Context, dbData env.DbData, td diff.TableDelta, fromRefDetails, toRefDetails *refDetails) ([]string, error) {
	// ToTable is used as target table as it cannot be nil at this point
	diffSch, projections, ri, err := getDiffQuery(ctx, dbData, td, fromRefDetails, toRefDetails)
	if err != nil {
		return nil, err
	}

	targetPkSch, err := sqlutil.FromDoltSchema(td.ToName, td.ToSch)
	if err != nil {
		return nil, err
	}

	return getDataSqlPatchResults(ctx, diffSch, targetPkSch.Schema, projections, ri, td.ToName, td.ToSch)
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

// getDiffQuery returns diff schema for specified columns and array of sql.Expression as projection to be used
// on diff table function row iter. This function attempts to imitate running a query
// fmt.Sprintf("select %s, %s from dolt_diff('%s', '%s', '%s')", columnsWithDiff, "diff_type", fromRef, toRef, tableName)
// on sql engine, which returns the schema and rowIter of the final data diff result.
func getDiffQuery(ctx *sql.Context, dbData env.DbData, td diff.TableDelta, fromRefDetails, toRefDetails *refDetails) (sql.Schema, []sql.Expression, sql.RowIter, error) {
	diffTableSchema, j, err := dtables.GetDiffTableSchemaAndJoiner(td.ToTable.Format(), td.FromSch, td.ToSch)
	if err != nil {
		return nil, nil, nil, err
	}
	diffPKSch, err := sqlutil.FromDoltSchema("", diffTableSchema)
	if err != nil {
		return nil, nil, nil, err
	}

	columnsWithDiff := getColumnNamesWithDiff(td.FromSch, td.ToSch)
	diffQuerySqlSch, projections := getDiffQuerySqlSchemaAndProjections(diffPKSch.Schema, columnsWithDiff)

	dp := dtables.NewDiffPartition(td.ToTable, td.FromTable, toRefDetails.hashStr, fromRefDetails.hashStr, toRefDetails.commitTime, fromRefDetails.commitTime, td.ToSch, td.FromSch)
	ri := dtables.NewDiffPartitionRowIter(*dp, dbData.Ddb, j)

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
			r := sql.Row{itr.statementIdx, itr.fromRef, itr.toRef, itr.currentPatch.tblName}
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
	diffType := "schema"
	if p.idx >= p.ddlLen {
		diffType = "data"
	}

	return sql.Row{diffType, stmt}, nil
}

func (p *patchStatementsRowIter) Close(_ *sql.Context) error {
	return nil
}
