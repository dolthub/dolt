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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

const diffTableDefaultRowCount = 1000

var ErrInvalidNonLiteralArgument = errors.NewKind("Invalid argument to %s: %s – only literal values supported")

var _ sql.TableFunction = (*DiffTableFunction)(nil)
var _ sql.ExecSourceRel = (*DiffTableFunction)(nil)

type DiffTableFunction struct {
	ctx            *sql.Context
	fromCommitExpr sql.Expression
	toCommitExpr   sql.Expression
	dotCommitExpr  sql.Expression
	tableNameExpr  sql.Expression
	database       sql.Database
	sqlSch         sql.Schema
	joiner         *rowconv.Joiner

	tableDelta diff.TableDelta
	fromDate   *types.Timestamp
	toDate     *types.Timestamp
}

// NewInstance creates a new instance of TableFunction interface
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

func (dtf *DiffTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(dtf.Schema())
	numRows, _, err := dtf.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (dtf *DiffTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return diffTableDefaultRowCount, false, nil
}

// Database implements the sql.Databaser interface
func (dtf *DiffTableFunction) Database() sql.Database {
	return dtf.database
}

// WithDatabase implements the sql.Databaser interface
func (dtf *DiffTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	ndtf := *dtf
	ndtf.database = database
	return &ndtf, nil
}

// Expressions implements the sql.Expressioner interface
func (dtf *DiffTableFunction) Expressions() []sql.Expression {
	if dtf.dotCommitExpr != nil {
		return []sql.Expression{
			dtf.dotCommitExpr, dtf.tableNameExpr,
		}
	}
	return []sql.Expression{
		dtf.fromCommitExpr, dtf.toCommitExpr, dtf.tableNameExpr,
	}
}

// WithExpressions implements the sql.Expressioner interface
func (dtf *DiffTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) < 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(dtf.Name(), "2 to 3", len(expression))
	}

	// TODO: For now, we will only support literal / fully-resolved arguments to the
	//       DiffTableFunction to avoid issues where the schema is needed in the analyzer
	//       before the arguments could be resolved.
	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(dtf.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(dtf.Name(), expr.String())
		}
	}

	newDtf := *dtf
	if strings.Contains(expression[0].String(), "..") {
		if len(expression) != 2 {
			return nil, sql.ErrInvalidArgumentNumber.New(fmt.Sprintf("%v with .. or ...", newDtf.Name()), 2, len(expression))
		}
		newDtf.dotCommitExpr = expression[0]
		newDtf.tableNameExpr = expression[1]
	} else {
		if len(expression) != 3 {
			return nil, sql.ErrInvalidArgumentNumber.New(newDtf.Name(), 3, len(expression))
		}
		newDtf.fromCommitExpr = expression[0]
		newDtf.toCommitExpr = expression[1]
		newDtf.tableNameExpr = expression[2]
	}

	fromCommitVal, toCommitVal, dotCommitVal, tableName, err := newDtf.evaluateArguments()
	if err != nil {
		return nil, err
	}

	err = newDtf.generateSchema(newDtf.ctx, fromCommitVal, toCommitVal, dotCommitVal, tableName)
	if err != nil {
		return nil, err
	}

	return &newDtf, nil
}

// Children implements the sql.Node interface
func (dtf *DiffTableFunction) Children() []sql.Node {
	return nil
}

// RowIter implements the sql.Node interface
func (dtf *DiffTableFunction) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	// Everything we need to start iterating was cached when we previously determined the schema of the result
	// TODO: When we add support for joining on table functions, we'll need to evaluate this against the
	//       specified row. That row is what has the left_table context in a join query.
	//       This will expand the test cases we need to cover significantly.
	fromCommitVal, toCommitVal, dotCommitVal, _, err := dtf.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := dtf.database.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unable to get dolt database")
	}

	fromCommitStr, toCommitStr, err := loadCommitStrings(ctx, fromCommitVal, toCommitVal, dotCommitVal, sqledb)
	if err != nil {
		return nil, err
	}

	ddb := sqledb.DbData().Ddb
	dp := dtables.NewDiffPartition(dtf.tableDelta.ToTable, dtf.tableDelta.FromTable, toCommitStr, fromCommitStr, dtf.toDate, dtf.fromDate, dtf.tableDelta.ToSch, dtf.tableDelta.FromSch)

	return dtables.NewDiffPartitionRowIter(dp, ddb, dtf.joiner), nil
}

// findMatchingDelta returns the best matching table delta for the table name
// given, taking renames into account
// TODO: schema name
func findMatchingDelta(deltas []diff.TableDelta, tableName string) diff.TableDelta {
	tableName = strings.ToLower(tableName)
	for _, d := range deltas {
		if strings.EqualFold(d.ToName.Name, tableName) {
			return d
		}
	}

	for _, d := range deltas {
		if strings.EqualFold(d.FromName.Name, tableName) {
			return d
		}
	}

	// no delta means no diff, or the table doesn't exist
	return diff.TableDelta{}
}

type refDetails struct {
	root       doltdb.RootValue
	hashStr    string
	commitTime *types.Timestamp
}

// loadDetailsForRef loads the root, hash, and timestamp for the specified from
// and to ref values
func loadDetailsForRefs(ctx *sql.Context, fromRef, toRef, dotRef interface{}, db dsess.SqlDatabase) (*refDetails, *refDetails, error) {
	fromCommitStr, toCommitStr, err := loadCommitStrings(ctx, fromRef, toRef, dotRef, db)
	if err != nil {
		return nil, nil, err
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbName := db.Name()

	fromRoot, fromCommitTime, fromHashStr, err := sess.ResolveRootForRef(ctx, dbName, fromCommitStr)
	if err != nil {
		return nil, nil, err
	}
	fromDetails := &refDetails{fromRoot, fromHashStr, fromCommitTime}

	toRoot, toCommitTime, toHashStr, err := sess.ResolveRootForRef(ctx, dbName, toCommitStr)
	if err != nil {
		return nil, nil, err
	}
	toDetails := &refDetails{toRoot, toHashStr, toCommitTime}

	return fromDetails, toDetails, nil
}

func resolveCommitStrings(ctx *sql.Context, fromRef, toRef, dotRef interface{}, db dsess.SqlDatabase) (string, string, error) {
	if dotRef != nil {
		dotStr, err := interfaceToString(dotRef)
		if err != nil {
			return "", "", err
		}

		sess := dsess.DSessFromSess(ctx.Session)

		if strings.Contains(dotStr, "...") {
			refs := strings.Split(dotStr, "...")

			headRef, err := sess.CWBHeadRef(ctx, db.Name())
			if err != nil {
				return "", "", err
			}

			rightCm, err := resolveCommit(ctx, db.DbData().Ddb, headRef, refs[0])
			if err != nil {
				return "", "", err
			}

			leftCm, err := resolveCommit(ctx, db.DbData().Ddb, headRef, refs[1])
			if err != nil {
				return "", "", err
			}

			mergeBase, err := merge.MergeBase(ctx, rightCm, leftCm)
			if err != nil {
				return "", "", err
			}

			return mergeBase.String(), refs[1], nil
		} else {
			refs := strings.Split(dotStr, "..")
			return refs[0], refs[1], nil
		}
	}

	fromStr, err := interfaceToString(fromRef)
	if err != nil {
		return "", "", err
	}

	toStr, err := interfaceToString(toRef)
	if err != nil {
		return "", "", err
	}

	return fromStr, toStr, nil
}

// loadCommitStrings gets the to and from commit strings, using the common
// ancestor as the from commit string for three dot diff
func loadCommitStrings(ctx *sql.Context, fromRef, toRef, dotRef interface{}, db dsess.SqlDatabase) (string, string, error) {
	fromStr, toStr, err := resolveCommitStrings(ctx, fromRef, toRef, dotRef, db)
	if err != nil {
		return "", "", err
	}

	if len(fromStr) == 0 || len(toStr) == 0 {
		return "", "", fmt.Errorf("expected strings for from and to revisions, got: %v, %v", fromStr, toStr)
	}

	return fromStr, toStr, nil
}

// interfaceToString converts an interface to a string
func interfaceToString(r interface{}) (string, error) {
	str, ok := r.(string)
	if !ok {
		return "", fmt.Errorf("received '%v' when expecting commit hash string", str)
	}
	return str, nil
}

func resolveRoot(ctx *sql.Context, sess *dsess.DoltSession, dbName, hashStr string) (*refDetails, error) {
	root, commitTime, _, err := sess.ResolveRootForRef(ctx, dbName, hashStr)
	if err != nil {
		return nil, err
	}

	return &refDetails{root, hashStr, commitTime}, nil
}

func resolveCommit(ctx *sql.Context, ddb *doltdb.DoltDB, headRef ref.DoltRef, cSpecStr string) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(cSpecStr)
	if err != nil {
		return nil, err
	}

	optCmt, err := ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return nil, err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	return cm, nil
}

// WithChildren implements the sql.Node interface
func (dtf *DiffTableFunction) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return dtf, nil
}

// CheckPrivileges implements the sql.Node interface
func (dtf *DiffTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	_, _, _, tableName, err := dtf.evaluateArguments()
	if err != nil {
		return false
	}

	subject := sql.PrivilegeCheckSubject{Database: dtf.database.Name(), Table: tableName}
	// TODO: Add tests for privilege checking
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
}

// evaluateArguments evaluates the argument expressions to turn them into
// values this DiffTableFunction can use. Note that this method only evals
// the expressions, and doesn't validate the values.
// TODO: evaluating expression arguments during binding is incompatible
// with prepared statement support.
func (dtf *DiffTableFunction) evaluateArguments() (interface{}, interface{}, interface{}, string, error) {
	if !dtf.Resolved() {
		return nil, nil, nil, "", nil
	}

	if !gmstypes.IsText(dtf.tableNameExpr.Type()) {
		return nil, nil, nil, "", sql.ErrInvalidArgumentDetails.New(dtf.Name(), dtf.tableNameExpr.String())
	}

	tableNameVal, err := dtf.tableNameExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return nil, nil, nil, "", err
	}

	tableName, ok := tableNameVal.(string)
	if !ok {
		return nil, nil, nil, "", ErrInvalidTableName.New(dtf.tableNameExpr.String())
	}

	if dtf.dotCommitExpr != nil {
		if !gmstypes.IsText(dtf.dotCommitExpr.Type()) {
			return nil, nil, nil, "", sql.ErrInvalidArgumentDetails.New(dtf.Name(), dtf.dotCommitExpr.String())
		}

		dotCommitVal, err := dtf.dotCommitExpr.Eval(dtf.ctx, nil)
		if err != nil {
			return nil, nil, nil, "", err
		}

		return nil, nil, dotCommitVal, tableName, nil
	}

	if !gmstypes.IsText(dtf.fromCommitExpr.Type()) {
		return nil, nil, nil, "", sql.ErrInvalidArgumentDetails.New(dtf.Name(), dtf.fromCommitExpr.String())
	}
	if !gmstypes.IsText(dtf.toCommitExpr.Type()) {
		return nil, nil, nil, "", sql.ErrInvalidArgumentDetails.New(dtf.Name(), dtf.toCommitExpr.String())
	}

	fromCommitVal, err := dtf.fromCommitExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return nil, nil, nil, "", err
	}

	toCommitVal, err := dtf.toCommitExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return nil, nil, nil, "", err
	}
	return fromCommitVal, toCommitVal, nil, tableName, nil
}

func (dtf *DiffTableFunction) generateSchema(ctx *sql.Context, fromCommitVal, toCommitVal, dotCommitVal interface{}, tableName string) error {
	if !dtf.Resolved() {
		return nil
	}

	sqledb, ok := dtf.database.(dsess.SqlDatabase)
	if !ok {
		return fmt.Errorf("unexpected database type: %T", dtf.database)
	}

	delta, err := dtf.cacheTableDelta(ctx, fromCommitVal, toCommitVal, dotCommitVal, tableName, sqledb)
	if err != nil {
		return err
	}

	fromTable, fromTableExists := delta.FromTable, delta.FromTable != nil
	toTable, toTableExists := delta.ToTable, delta.ToTable != nil

	if !toTableExists && !fromTableExists {
		return sql.ErrTableNotFound.New(tableName)
	}

	var toSchema, fromSchema schema.Schema
	var format *types.NomsBinFormat

	if fromTableExists {
		fromSchema = delta.FromSch
		format = fromTable.Format()
	}

	if toTableExists {
		toSchema = delta.ToSch
		format = toTable.Format()
	}

	diffTableSch, j, err := dtables.GetDiffTableSchemaAndJoiner(format, fromSchema, toSchema)
	if err != nil {
		return err
	}
	dtf.joiner = j

	// TODO: sql.Columns include a Source that indicates the table it came from, but we don't have a real table
	//       when the column comes from a table function, so we omit the table name when we create these columns.
	//       This allows column projections to work correctly with table functions, but we will need to add a
	//       unique id (e.g. hash generated from method arguments) when we add support for aliasing and joining
	//       table functions in order for the analyzer to determine which table function result a column comes from.
	sqlSchema, err := sqlutil.FromDoltSchema("", "", diffTableSch)
	if err != nil {
		return err
	}

	dtf.sqlSch = sqlSchema.Schema

	return nil
}

// cacheTableDelta caches and returns an appropriate table delta for the table name given, taking renames into
// consideration. Returns a sql.ErrTableNotFound if the given table name cannot be found in either revision.
func (dtf *DiffTableFunction) cacheTableDelta(ctx *sql.Context, fromCommitVal, toCommitVal, dotCommitVal interface{}, tableName string, db dsess.SqlDatabase) (diff.TableDelta, error) {
	fromRefDetails, toRefDetails, err := loadDetailsForRefs(ctx, fromCommitVal, toCommitVal, dotCommitVal, db)
	if err != nil {
		return diff.TableDelta{}, err
	}

	fromTableName, fromTable, fromTableExists, err := resolve.Table(ctx, fromRefDetails.root, tableName)
	if err != nil {
		return diff.TableDelta{}, err
	}

	toTableName, toTable, toTableExists, err := resolve.Table(ctx, toRefDetails.root, tableName)
	if err != nil {
		return diff.TableDelta{}, err
	}

	if !fromTableExists && !toTableExists {
		return diff.TableDelta{}, sql.ErrTableNotFound.New(tableName)
	}

	// TODO: it would be nice to limit this to just the table under consideration, not all tables with a diff
	deltas, err := diff.GetTableDeltas(ctx, fromRefDetails.root, toRefDetails.root)
	if err != nil {
		return diff.TableDelta{}, err
	}

	dtf.fromDate = fromRefDetails.commitTime
	dtf.toDate = toRefDetails.commitTime

	delta := findMatchingDelta(deltas, tableName)

	// We only get a delta if there's a diff. When there isn't one, construct a delta here with table and schema info
	// TODO: schema name
	if delta.FromTable == nil && delta.ToTable == nil {
		delta.FromName = fromTableName
		delta.ToName = toTableName
		delta.FromTable = fromTable
		delta.ToTable = toTable

		if fromTable != nil {
			sch, err := fromTable.GetSchema(ctx)
			if err != nil {
				return diff.TableDelta{}, err
			}
			delta.FromSch = sch
		}

		if toTable != nil {
			sch, err := toTable.GetSchema(ctx)
			if err != nil {
				return diff.TableDelta{}, err
			}
			delta.ToSch = sch
		}

		// TODO: There are other fields we could set here that we don't
	}

	dtf.tableDelta = delta

	return delta, nil
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
	if dtf.dotCommitExpr != nil {
		return dtf.tableNameExpr.Resolved() && dtf.dotCommitExpr.Resolved()
	}
	return dtf.tableNameExpr.Resolved() && dtf.fromCommitExpr.Resolved() && dtf.toCommitExpr.Resolved()
}

func (dtf *DiffTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (dtf *DiffTableFunction) String() string {
	if dtf.dotCommitExpr != nil {
		return fmt.Sprintf("DOLT_DIFF(%s, %s)",
			dtf.dotCommitExpr.String(),
			dtf.tableNameExpr.String())
	}
	return fmt.Sprintf("DOLT_DIFF(%s, %s, %s)",
		dtf.fromCommitExpr.String(),
		dtf.toCommitExpr.String(),
		dtf.tableNameExpr.String())
}

// Name implements the sql.TableFunction interface
func (dtf *DiffTableFunction) Name() string {
	return "dolt_diff"
}
