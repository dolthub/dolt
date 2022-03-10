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
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.TableFunction = (*DiffTableFunction)(nil)

// TODO: Audit these fields and see if we really need all of them
type DiffTableFunction struct {
	ctx            *sql.Context
	tableNameExpr  sql.Expression
	toCommitExpr   sql.Expression
	fromCommitExpr sql.Expression
	tableName      string
	toCommitVal    interface{}
	fromCommitVal  interface{}
	database       sql.Database
	sch            schema.Schema // TODO: Audit
	sqlSch         sql.Schema    // TODO: Audit
	joiner         *rowconv.Joiner
	toSch          schema.Schema
	fromSch        schema.Schema
}

func (dtf *DiffTableFunction) Database() sql.Database {
	return dtf.database
}

func (dtf *DiffTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	dtf.database = database

	return dtf, nil
}

func (dtf *DiffTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{dtf.tableNameExpr, dtf.fromCommitExpr, dtf.toCommitExpr}
}

func (dtf *DiffTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) != 3 {
		return nil, errors.New("invalid number of expressions passed to function dolt_diff. " +
			"expected 3, but received " + string(len(expression)))
	}

	if !sql.IsText(expression[0].Type()) {
		// TODO: Error handling
		panic("invalid argument passed to function")
	}

	t := expression[1].Type()
	if !sql.IsText(t) {
		// TODO: Error handling
		panic("invalid argument passed to function")
	}

	if !sql.IsText(expression[2].Type()) {
		// TODO: Error handling
		panic("invalid argument passed to function")
	}

	dtf.tableNameExpr = expression[0]
	dtf.fromCommitExpr = expression[1]
	dtf.toCommitExpr = expression[2]

	return dtf, nil
}

func (dtf *DiffTableFunction) Children() []sql.Node {
	return []sql.Node{}
}

// TODO: Copied from diff_table
func toNamer(name string) string {
	return diff.To + "_" + name
}

// TODO: Copied from diff_table
func fromNamer(name string) string {
	return diff.From + "_" + name
}

// loadCommits loads the toCommit and fromCommit arguments as doltdb.Commit structures.
func (dtf *DiffTableFunction) loadCommits() (*doltdb.Commit, *doltdb.Commit, error) {
	sqledb, ok := dtf.database.(Database)
	if !ok {
		panic("unable to get dolt database")
	}
	ddb := sqledb.GetDoltDB()

	fromCommitString := fmt.Sprint(dtf.fromCommitVal)
	fromCommitSpec, err := doltdb.NewCommitSpec(fromCommitString)
	if err != nil {
		return nil, nil, err
	}

	toCommitString := fmt.Sprint(dtf.toCommitVal)
	toCommitSpec, err := doltdb.NewCommitSpec(toCommitString)
	if err != nil {
		return nil, nil, err
	}

	//fromCommitHash, err := fromCommit.HashOf()
	//if err != nil {
	//	return nil, nil, err
	//}

	fromCommit, err := ddb.Resolve(dtf.ctx, fromCommitSpec, nil)
	if err != nil {
		return nil, nil, err
	}

	toCommit, err := ddb.Resolve(dtf.ctx, toCommitSpec, nil)
	if err != nil {
		return nil, nil, err
	}

	return fromCommit, toCommit, nil
}

func (dtf *DiffTableFunction) newCommitItrForCommitRange(fromCommit, toCommit *doltdb.Commit) (*doltdb.CommitItr, error) {
	fromCommitHash, err := fromCommit.HashOf()
	if err != nil {
		return nil, err
	}

	lastCommitSeen := false
	myFunc := func(ctx context.Context, h hash.Hash, commit *doltdb.Commit) (filterOut bool, err error) {
		if lastCommitSeen {
			return true, nil
		}

		if h == fromCommitHash {
			lastCommitSeen = true
		}

		return false, nil
	}

	sqledb, ok := dtf.database.(Database)
	if !ok {
		panic("unable to get dolt database")
	}
	ddb := sqledb.GetDoltDB()

	commitItr := doltdb.CommitItrForRoots(ddb, toCommit)
	commitItr = doltdb.NewFilteringCommitItr(commitItr, myFunc)

	return &commitItr, nil
}

// initializeCommitHashToTableMap creates the commit hash to table map needed by DiffPartitions by
// pulling the first commit from the specified doltdb.CommitItr and populating that commit's
// entry in the returned map. Without specifying this information, the Diff processor will
// not be able to process the first commit it sees.
func (dtf *DiffTableFunction) initializeCommitHashToTableMap(commitItr *doltdb.CommitItr) (*map[hash.Hash]dtables.TblInfoAtCommit, error) {
	sqledb, ok := dtf.database.(Database)
	if !ok {
		panic("unable to get dolt database")
	}

	firstCmHash, _, err := (*commitItr).Next(dtf.ctx)
	if err != nil {
		return nil, err
	}
	err = (*commitItr).Reset(dtf.ctx)
	if err != nil {
		return nil, err
	}

	toRoot, err := sqledb.RootAsOf(dtf.ctx, dtf.toCommitVal)
	if err != nil {
		return nil, err
	}

	t, exactName, ok, err := toRoot.GetTableInsensitive(dtf.ctx, dtf.tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		// TODO:
		panic("unable to find table")
	}

	wrTblHash, _, err := toRoot.GetTableHash(dtf.ctx, exactName)
	if err != nil {
		return nil, err
	}

	cmHashToTblInfo := make(map[hash.Hash]dtables.TblInfoAtCommit)
	cmHashToTblInfo[firstCmHash] = dtables.NewTblInfoAtCommit("WORKING", nil, t, wrTblHash)

	return &cmHashToTblInfo, nil
}

func (dtf *DiffTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	err := dtf.evaluateArguments()
	if err != nil {
		return nil, err
	}

	err = dtf.validateArguments()
	if err != nil {
		return nil, err
	}

	if dtf.joiner == nil {
		panic("schema and joiner haven't been intialized!")
	}

	sqledb, ok := dtf.database.(Database)
	if !ok {
		panic("unable to get dolt database")
	}
	ddb := sqledb.GetDoltDB()

	fromCommit, toCommit, err := dtf.loadCommits()
	if err != nil {
		return nil, err
	}

	commitItr, err := dtf.newCommitItrForCommitRange(fromCommit, toCommit)
	if err != nil {
		return nil, err
	}

	// TODO: Do we need this if the filters are nil?
	sf, err := dtables.SelectFuncForFilters(ddb.Format(), nil)
	if err != nil {
		return nil, err
	}

	cmHashToTblInfo, err := dtf.initializeCommitHashToTableMap(commitItr)
	if err != nil {
		return nil, err
	}

	diffPartitions := dtables.NewDiffPartitions(dtf.tableName, *commitItr, *cmHashToTblInfo, sf, dtf.toSch, dtf.fromSch)

	return NewDiffTableFunctionRowIter(diffPartitions, ddb, dtf.joiner), nil
}

func (dtf *DiffTableFunction) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) != 0 {
		panic("unexpected children")
	}
	return dtf, nil
}

func (dtf *DiffTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// evaluateArguments evaluates the argument expressions to turn them into values this DiffTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (dtf *DiffTableFunction) evaluateArguments() error {
	tableNameVal, err := dtf.tableNameExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return err
	}
	tableName, ok := tableNameVal.(string)
	if !ok {
		// TODO: error handling
		panic("unable to convert tablename to string")
	}
	dtf.tableName = tableName

	fromCommitVal, err := dtf.fromCommitExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return nil
	}
	dtf.fromCommitVal = fromCommitVal

	toCommitVal, err := dtf.toCommitExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return nil
	}
	dtf.toCommitVal = toCommitVal

	return nil
}

func (dtf *DiffTableFunction) validateArguments() error {

	// TODO: Validate arguments
	//       - is tableName a valid table?
	//       - is fromCommit a valid commit? (or ref?)
	//       - is toCommit a valid commit? (or ref?)
	//       - is fromCommit an ancestor of toCommit?

	return nil
}

func (dtf *DiffTableFunction) generateSchema() sql.Schema {
	err := dtf.evaluateArguments()
	if err != nil {
		panic(fmt.Sprintf("unable to generate schema: %v", err))
	}

	sqledb, ok := dtf.database.(Database)
	if !ok {
		panic(fmt.Sprintf("unexpected database type: %T", dtf.database))
	}

	fromRoot, err := sqledb.RootAsOf(dtf.ctx, dtf.fromCommitVal)
	if err != nil {
		return nil
	}

	fromTable, ok, err := fromRoot.GetTable(dtf.ctx, dtf.tableName)
	if err != nil {
		return nil
	}
	if !ok {
		panic("unable to load from table!")
	}

	toRoot, err := sqledb.RootAsOf(dtf.ctx, dtf.toCommitVal)
	if err != nil {
		return nil
	}

	toTable, ok, err := toRoot.GetTable(dtf.ctx, dtf.tableName)
	if err != nil {
		return nil
	}
	if !ok {
		panic("unable to load to table!")
	}

	fromSchema, err := fromTable.GetSchema(dtf.ctx)
	if err != nil {
		return nil
	}

	toSchema, err := toTable.GetSchema(dtf.ctx)
	if err != nil {
		return nil
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
			diff.To:   diff.ToNamer,
			diff.From: diff.FromNamer,
		})
	if err != nil {
		return nil
	}

	sch := joiner.GetSchema()

	sch = schema.MustSchemaFromCols(
		sch.GetAllCols().Append(
			schema.NewColumn("diff_type", schema.DiffTypeTag, types.StringKind, false)))

	// TODO: sql.Columns include a Source that indicates the table it came from, but we don't have a real table
	//       when the column comes from a table function, so we omit the table name when we create these columns.
	//       This allows column projections to work correctly with table functions, but we should test that this
	//       works in more complex scenarios (e.g. projections with multiple table functions in the same statement)
	//       and make sure we don't need to create a unique identifier for each use of this table function.
	sqlSchema, err := sqlutil.FromDoltSchema("", sch)
	if err != nil {
		// TODO: return err instead
		panic(err)
	}

	dtf.sch = sch
	dtf.joiner = joiner

	return sqlSchema.Schema
}

// Schema implements the Node interface
func (dtf *DiffTableFunction) Schema() sql.Schema {
	if !dtf.Resolved() {
		return nil
	}

	if dtf.sqlSch == nil {
		dtf.sqlSch = dtf.generateSchema()
	}

	return dtf.sqlSch
}

// Resolved implements the Resolvable interface
func (dtf *DiffTableFunction) Resolved() bool {
	return dtf.tableNameExpr.Resolved() && dtf.fromCommitExpr.Resolved() && dtf.toCommitExpr.Resolved()
}

// String implements the Stringer interface
func (dtf *DiffTableFunction) String() string {
	return "dolt diff table function"
}

// TableFunctionName implements the sql.TableFunction interface
func (dtf *DiffTableFunction) TableFunctionName() string {
	return "dolt_diff"
}

// Description implements the sql.TableFunction interface
func (dtf *DiffTableFunction) Description() string {
	return "returns dolt diff data as a relational table"
}

func (dtf *DiffTableFunction) WithContext(ctx *sql.Context) *DiffTableFunction {
	dtf.ctx = ctx

	return dtf
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
			continue
		} else if err != nil {
			return nil, err
		} else {
			return row, nil
		}
	}
}

func (itr *diffTableFunctionRowIter) Close(ctx *sql.Context) error {
	return nil
}
