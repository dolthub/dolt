// Copyright 2020-2021 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.TableFunction = (*DiffTableFunction)(nil)

type DiffTableFunction struct {
	ctx            *sql.Context
	tableNameExpr  sql.Expression
	toCommitExpr   sql.Expression
	fromCommitExpr sql.Expression
	tableName      string
	toCommitVal    interface{}
	fromCommitVal  interface{}
	database       sql.Database
	sqlSch         sql.Schema
	joiner         *rowconv.Joiner
	toSch          schema.Schema
	fromSch        schema.Schema
}

// NewInstance implements the TableFunction interface
func (dtf *DiffTableFunction) NewInstance(ctx *sql.Context, database sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &DiffTableFunction{
		ctx:      ctx,
		database: database,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Database implements the sql.Databaser interface
func (dtf *DiffTableFunction) Database() sql.Database {
	return dtf.database
}

// WithDatabase implements the sql.Databaser interface
func (dtf *DiffTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	dtf.database = database

	return dtf, nil
}

// Expressions implements the sql.Expressioner interface
func (dtf *DiffTableFunction) Expressions() []sql.Expression {
	expressions := make([]sql.Expression, 0, 3)

	if dtf.tableNameExpr != nil {
		expressions = append(expressions, dtf.tableNameExpr)
	}

	if dtf.fromCommitExpr != nil {
		expressions = append(expressions, dtf.fromCommitExpr)
	}

	if dtf.toCommitExpr != nil {
		expressions = append(expressions, dtf.toCommitExpr)
	}

	return expressions
}

// WithExpressions implements the sql.Expressioner interface
func (dtf *DiffTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New(dtf.FunctionName(), 3, len(expression))
	}

	dtf.tableNameExpr = expression[0]
	dtf.fromCommitExpr = expression[1]
	dtf.toCommitExpr = expression[2]

	err := dtf.evaluateArguments()
	if err != nil {
		return nil, err
	}

	return dtf, nil
}

// Children implements the sql.Node interface
func (dtf *DiffTableFunction) Children() []sql.Node {
	return nil
}

// RowIter implements the sql.Node interface
func (dtf *DiffTableFunction) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	err := dtf.evaluateArguments()
	if err != nil {
		return nil, err
	}

	if dtf.joiner == nil {
		panic("schema and joiner haven't been initialized")
	}

	sqledb, ok := dtf.database.(Database)
	if !ok {
		panic("unable to get dolt database")
	}
	ddb := sqledb.GetDoltDB()

	toRoot, toHash, toDate, err := dtf.loadDetailsForRef(ctx, dtf.toCommitVal, ddb)
	if err != nil {
		return nil, err
	}

	toTable, _, err := toRoot.GetTable(ctx, dtf.tableName)
	if err != nil {
		return nil, err
	}

	fromRoot, fromHash, fromDate, err := dtf.loadDetailsForRef(ctx, dtf.fromCommitVal, ddb)
	if err != nil {
		return nil, err
	}

	fromTable, _, err := fromRoot.GetTable(ctx, dtf.tableName)
	if err != nil {
		return nil, err
	}

	dp := dtables.NewDiffPartition(toTable, fromTable, toHash, fromHash, toDate, fromDate, &dtf.toSch, &dtf.fromSch)

	return NewDiffTableFunctionRowIterForSinglePartition(*dp, ddb, dtf.joiner), nil
}

// loadDetailsForRef loads the root, hash, and timestamp for the specified ref value
func (dtf *DiffTableFunction) loadDetailsForRef(ctx *sql.Context, ref interface{}, ddb *doltdb.DoltDB) (*doltdb.RootValue, string, *types.Timestamp, error) {
	hashStr, ok := ref.(string)
	if !ok {
		return nil, "", nil, fmt.Errorf("received '%v' when expecting commit hash string", ref)
	}

	var root *doltdb.RootValue
	var commitTime *types.Timestamp
	cs, err := doltdb.NewCommitSpec(hashStr)
	if err != nil {
		return nil, "", nil, err
	}

	cm, err := ddb.Resolve(ctx, cs, nil)
	if err != nil {
		return nil, "", nil, err
	}

	root, err = cm.GetRootValue()
	if err != nil {
		return nil, "", nil, err
	}

	meta, err := cm.GetCommitMeta()
	if err != nil {
		return nil, "", nil, err
	}

	t := meta.Time()
	commitTime = (*types.Timestamp)(&t)

	return root, hashStr, commitTime, nil
}

// WithChildren implements the sql.Node interface
func (dtf *DiffTableFunction) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) != 0 {
		panic("unexpected children")
	}
	return dtf, nil
}

// CheckPrivileges implements the sql.Node interface
func (dtf *DiffTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// TODO: Add tests for privilege checking
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(dtf.database.Name(), dtf.tableName, "", sql.PrivilegeType_Select))
}

// evaluateArguments evaluates the argument expressions to turn them into values this DiffTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (dtf *DiffTableFunction) evaluateArguments() error {
	if !dtf.Resolved() {
		return nil
	}

	if !sql.IsText(dtf.tableNameExpr.Type()) {
		return sql.ErrInvalidArgumentDetails.New(dtf.FunctionName(), dtf.tableNameExpr.String())
	}

	if !sql.IsText(dtf.fromCommitExpr.Type()) {
		return sql.ErrInvalidArgumentDetails.New(dtf.FunctionName(), dtf.fromCommitExpr.String())
	}

	if !sql.IsText(dtf.toCommitExpr.Type()) {
		return sql.ErrInvalidArgumentDetails.New(dtf.FunctionName(), dtf.toCommitExpr.String())
	}

	tableNameVal, err := dtf.tableNameExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return err
	}
	tableName, ok := tableNameVal.(string)
	if !ok {
		return ErrInvalidTableName.New(dtf.tableNameExpr.String())
	}
	dtf.tableName = tableName

	fromCommitVal, err := dtf.fromCommitExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return err
	}
	dtf.fromCommitVal = fromCommitVal

	toCommitVal, err := dtf.toCommitExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return err
	}
	dtf.toCommitVal = toCommitVal

	dtf.sqlSch, err = dtf.generateSchema()
	if err != nil {
		return err
	}

	return nil
}

func (dtf *DiffTableFunction) generateSchema() (sql.Schema, error) {
	if !dtf.Resolved() {
		return nil, nil
	}

	sqledb, ok := dtf.database.(Database)
	if !ok {
		panic(fmt.Sprintf("unexpected database type: %T", dtf.database))
	}

	fromRoot, err := sqledb.rootAsOf(dtf.ctx, dtf.fromCommitVal)
	if err != nil {
		return nil, err
	}

	fromTable, ok, err := fromRoot.GetTable(dtf.ctx, dtf.tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(dtf.tableName)
	}

	toRoot, err := sqledb.rootAsOf(dtf.ctx, dtf.toCommitVal)
	if err != nil {
		return nil, err
	}

	toTable, ok, err := toRoot.GetTable(dtf.ctx, dtf.tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(dtf.tableName)
	}

	fromSchema, err := fromTable.GetSchema(dtf.ctx)
	if err != nil {
		return nil, err
	}

	toSchema, err := toTable.GetSchema(dtf.ctx)
	if err != nil {
		return nil, err
	}

	fromSchema = schema.MustSchemaFromCols(
		fromSchema.GetAllCols().Append(
			schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
			schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false)))
	dtf.fromSch = fromSchema

	toSchema = schema.MustSchemaFromCols(
		toSchema.GetAllCols().Append(
			schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
			schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false)))
	dtf.toSch = toSchema

	joiner, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{{Name: diff.To, Sch: toSchema}, {Name: diff.From, Sch: fromSchema}},
		map[string]rowconv.ColNamingFunc{
			diff.To:   diff.ToColNamer,
			diff.From: diff.FromColNamer,
		})
	if err != nil {
		return nil, err
	}

	sch := joiner.GetSchema()

	sch = schema.MustSchemaFromCols(
		sch.GetAllCols().Append(
			schema.NewColumn("diff_type", schema.DiffTypeTag, types.StringKind, false)))

	// TODO: sql.Columns include a Source that indicates the table it came from, but we don't have a real table
	//       when the column comes from a table function, so we omit the table name when we create these columns.
	//       This allows column projections to work correctly with table functions, but we should test that this
	//       works in more complex scenarios (e.g. projections with multiple table functions in the same statement)
	//       and see if we need to create a unique identifier for each use of this table function.
	sqlSchema, err := sqlutil.FromDoltSchema("", sch)
	if err != nil {
		return nil, err
	}

	dtf.joiner = joiner

	return sqlSchema.Schema, nil
}

// Schema implements the sql.Node interface
func (dtf *DiffTableFunction) Schema() sql.Schema {
	if !dtf.Resolved() {
		return nil
	}

	if dtf.sqlSch == nil {
		panic("schema hasn't been generated yet")
	}

	return dtf.sqlSch
}

// Resolved implements the sql.Resolvable interface
func (dtf *DiffTableFunction) Resolved() bool {
	resolved := true

	if dtf.tableNameExpr != nil {
		resolved = resolved && dtf.tableNameExpr.Resolved()
	}

	if dtf.fromCommitExpr != nil {
		resolved = resolved && dtf.fromCommitExpr.Resolved()
	}

	if dtf.toCommitExpr != nil {
		resolved = resolved && dtf.toCommitExpr.Resolved()
	}

	return resolved
}

// String implements the Stringer interface
func (dtf *DiffTableFunction) String() string {
	return "dolt diff table function"
}

// FunctionName implements the sql.TableFunction interface
func (dtf *DiffTableFunction) FunctionName() string {
	return "dolt_diff"
}

//------------------------------------
// diffTableFunctionRowIter
//------------------------------------

var _ sql.RowIter = (*diffTableFunctionRowIter)(nil)

type diffTableFunctionRowIter struct {
	diffPartitions   *dtables.DiffPartitions
	ddb              *doltdb.DoltDB
	joiner           *rowconv.Joiner
	currentPartition *sql.Partition
	currentRowIter   *sql.RowIter
}

func NewDiffTableFunctionRowIter(partitions *dtables.DiffPartitions, ddb *doltdb.DoltDB, joiner *rowconv.Joiner) *diffTableFunctionRowIter {
	return &diffTableFunctionRowIter{
		diffPartitions: partitions,
		ddb:            ddb,
		joiner:         joiner,
	}
}

func NewDiffTableFunctionRowIterForSinglePartition(partition sql.Partition, ddb *doltdb.DoltDB, joiner *rowconv.Joiner) *diffTableFunctionRowIter {
	return &diffTableFunctionRowIter{
		currentPartition: &partition,
		ddb:              ddb,
		joiner:           joiner,
	}
}

func (itr *diffTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if itr.currentPartition == nil {
			nextPartition, err := itr.diffPartitions.Next(ctx)
			if err != nil {
				return nil, err
			}
			itr.currentPartition = &nextPartition
		}

		if itr.currentRowIter == nil {
			dp := (*itr.currentPartition).(dtables.DiffPartition)
			rowIter, err := dp.GetRowIter(ctx, itr.ddb, itr.joiner)
			if err != nil {
				return nil, err
			}
			itr.currentRowIter = &rowIter
		}

		row, err := (*itr.currentRowIter).Next(ctx)
		if err == io.EOF {
			itr.currentPartition = nil
			itr.currentRowIter = nil

			if itr.diffPartitions == nil {
				return nil, err
			}

			continue
		} else if err != nil {
			return nil, err
		} else {
			return row, nil
		}
	}
}

func (itr *diffTableFunctionRowIter) Close(_ *sql.Context) error {
	return nil
}
