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

package sqle

import (
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"io"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var _ sql.TableFunction = (*SchemaDiffTableFunction)(nil)
var _ sql.ExecSourceRel = (*SchemaDiffTableFunction)(nil)

type SchemaDiffTableFunction struct {
	ctx *sql.Context

	fromCommitExpr sql.Expression
	toCommitExpr   sql.Expression
	dotCommitExpr  sql.Expression
	tableNameExpr  sql.Expression
	database       sql.Database
	tableName      string
}

var schemaDiffTableSchema = sql.Schema{
	&sql.Column{Name: "from_table_name", Type: types.LongText, Nullable: false},
	&sql.Column{Name: "to_table_name", Type: types.LongText, Nullable: false},
	&sql.Column{Name: "from_create_statement", Type: types.Text, Nullable: false},
	&sql.Column{Name: "to_create_statement", Type: types.Text, Nullable: false},
	&sql.Column{Name: "from_table_hash", Type: types.Text, Nullable: false},
	&sql.Column{Name: "to_table_hash", Type: types.Text, Nullable: false},
}

// NewInstance creates a new instance of TableFunction interface
func (ds *SchemaDiffTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &SchemaDiffTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	ds = node.(*SchemaDiffTableFunction)

	// set the table name
	if ds.tableNameExpr == nil {
		return nil, fmt.Errorf("table name is required")
	}
	if !types.IsText(ds.tableNameExpr.Type()) {
		return nil, fmt.Errorf("table name expression must be a string")
	}
	tableNameVal, err := ds.tableNameExpr.Eval(ds.ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("error evaluating table name expression: %w", err)
	}
	tableName, ok := tableNameVal.(string)
	if !ok {
		return nil, fmt.Errorf("table name expression must evaluate to a string")
	}
	ds.tableName = tableName

	return ds, nil
}

// Database implements the sql.Databaser interface
func (ds *SchemaDiffTableFunction) Database() sql.Database {
	return ds.database
}

// WithDatabase implements the sql.Databaser interface
func (ds *SchemaDiffTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	nds := *ds
	nds.database = database
	return &nds, nil
}

// Name implements the sql.TableFunction interface
func (ds *SchemaDiffTableFunction) Name() string {
	return "dolt_schema_diff"
}

func (ds *SchemaDiffTableFunction) commitsResolved() bool {
	if ds.dotCommitExpr != nil {
		return ds.dotCommitExpr.Resolved()
	}
	return ds.fromCommitExpr.Resolved() && ds.toCommitExpr.Resolved()
}

// Resolved implements the sql.Resolvable interface
func (ds *SchemaDiffTableFunction) Resolved() bool {
	if ds.tableNameExpr != nil {
		return ds.commitsResolved() && ds.tableNameExpr.Resolved()
	}
	return ds.commitsResolved()
}

// String implements the Stringer interface
func (ds *SchemaDiffTableFunction) String() string {
	if ds.dotCommitExpr != nil {
		return fmt.Sprintf("DOLT_SCHEMA_DIFF(%s, %s)", ds.tableName, ds.dotCommitExpr.String(), ds.tableNameExpr.String())
	}
	return fmt.Sprintf("DOLT_SCHEMA_DIFF(%s, %s, %s)", ds.tableName, ds.fromCommitExpr.String(), ds.toCommitExpr.String(), ds.tableNameExpr.String())
}

// Schema implements the sql.Node interface.
func (ds *SchemaDiffTableFunction) Schema() sql.Schema {
	return schemaDiffTableSchema
}

// Children implements the sql.Node interface.
func (ds *SchemaDiffTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (ds *SchemaDiffTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return ds, nil
}

// CheckPrivileges implements the interface sql.Node.
func (ds *SchemaDiffTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if !types.IsText(ds.tableNameExpr.Type()) {
		return false
	}

	// TODO: Add tests for privilege checking
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(ds.database.Name(), ds.tableName, "", sql.PrivilegeType_Select))
}

// Expressions implements the sql.Expressioner interface.
func (ds *SchemaDiffTableFunction) Expressions() []sql.Expression {
	exprs := []sql.Expression{}
	if ds.dotCommitExpr != nil {
		exprs = append(exprs, ds.dotCommitExpr)
	} else {
		exprs = append(exprs, ds.fromCommitExpr, ds.toCommitExpr)
	}
	if ds.tableNameExpr != nil {
		exprs = append(exprs, ds.tableNameExpr)
	}
	return exprs
}

// WithExpressions implements the sql.Expressioner interface.
func (ds *SchemaDiffTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) < 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(ds.Name(), "2 to 3", len(expression))
	}

	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(ds.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(ds.Name(), expr.String())
		}
	}

	newDstf := *ds
	if strings.Contains(expression[0].String(), "..") {
		if len(expression) != 2 {
			return nil, sql.ErrInvalidArgumentNumber.New(newDstf.Name(), "2", len(expression))
		}
		newDstf.dotCommitExpr = expression[0]
		newDstf.tableNameExpr = expression[1]
	} else {
		if len(expression) != 3 {
			return nil, sql.ErrInvalidArgumentNumber.New(newDstf.Name(), "3", len(expression))
		}
		newDstf.fromCommitExpr = expression[0]
		newDstf.toCommitExpr = expression[1]
		newDstf.tableNameExpr = expression[2]
	}

	// validate the expressions
	if newDstf.dotCommitExpr != nil {
		if !types.IsText(newDstf.dotCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.dotCommitExpr.String())
		}
	} else {
		if !types.IsText(newDstf.fromCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.fromCommitExpr.String())
		}
		if !types.IsText(newDstf.toCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.toCommitExpr.String())
		}
	}

	if !types.IsText(newDstf.tableNameExpr.Type()) {
		return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.tableNameExpr.String())
	}

	return &newDstf, nil
}

// RowIter implements the sql.Node interface
func (ds *SchemaDiffTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	fromCommitVal, toCommitVal, dotCommitVal, tableName, err := ds.evaluateArguments()
	if err != nil {
		return nil, err
	}

	db := ds.database
	sess := dsess.DSessFromSess(ctx.Session)
	dbName := db.Name()

	sqledb, ok := ds.database.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", ds.database)
	}
	//engine := sqledb.

	fromCommitStr, toCommitStr, err := loadCommitStrings(ctx, fromCommitVal, toCommitVal, dotCommitVal, sqledb)
	if err != nil {
		return nil, err
	}

	fromRoot, _, _, err := sess.ResolveRootForRef(ctx, dbName, fromCommitStr)
	if err != nil {
		return nil, err
	}
	toRoot, _, _, err := sess.ResolveRootForRef(ctx, dbName, toCommitStr)
	if err != nil {
		return nil, err
	}

	//fromRoot, ok := diff.MaybeResolveRoot(ctx, sqledb.DbData().Rsr, sqledb.DbData().Ddb, fromCommitStr)
	//if !ok {
	//	return nil, fmt.Errorf("failed to resolve from commit: %s", fromCommitStr)
	//}
	//toRoot, ok := diff.MaybeResolveRoot(ctx, sqledb.DbData().Rsr, sqledb.DbData().Ddb, toCommitStr)
	//if !ok {
	//	return nil, fmt.Errorf("failed to resolve to commit: %s", toCommitStr)
	//}

	deltas, err := diff.GetTableDeltas(ctx, fromRoot, toRoot)
	if err != nil {
		return nil, err
	}

	sort.Slice(deltas, func(i, j int) bool {
		return strings.Compare(deltas[i].ToName, deltas[j].ToName) < 0
	})

	// If tableNameExpr defined, return a single table diff summary result
	delta := findMatchingDelta(deltas, tableName)

	var fromCreate, toCreate, fromHash, toHash string

	if delta.FromTable != nil {
		fromSqlDb := NewUserSpaceDatabase(fromRoot, editor.Options{})
		fromSqlCtx, fromEngine, _ := PrepareCreateTableStmt(ctx, fromSqlDb)
		fromCreate, err = GetCreateTableStmt(fromSqlCtx, fromEngine, delta.FromName)
		if err != nil {
			return nil, err
		}
		fromHashBytes, err := delta.FromTable.HashOf()
		if err != nil {
			return nil, err
		}
		fromHash = fromHashBytes.String()
	}

	if delta.ToTable != nil {
		toSqlDb := NewUserSpaceDatabase(toRoot, editor.Options{})
		toSqlCtx, toEngine, _ := PrepareCreateTableStmt(ctx, toSqlDb)
		toCreate, err = GetCreateTableStmt(toSqlCtx, toEngine, delta.ToName)
		if err != nil {
			return nil, err
		}
		toHashBytes, err := delta.ToTable.HashOf()
		if err != nil {
			return nil, err
		}
		toHash = toHashBytes.String()
	}

	iter := &schemaDiffTableFunctionRowIter{
		isDone:        false,
		fromTableName: delta.FromName,
		toTableName:   delta.ToName,
		fromCreate:    fromCreate,
		toCreate:      toCreate,
		fromHash:      fromHash,
		toHash:        toHash,
	}

	return iter, nil

	//summ, err := getSummaryForDelta(ctx, delta, sqledb, fromDetails, toDetails, true)
	//if err != nil {
	//	return nil, err
	//}
	//
	//summs := []*diff.TableDeltaSummary{}
	//if summ != nil {
	//	summs = []*diff.TableDeltaSummary{summ}
	//}
	//
	//return NewDiffSummaryTableFunctionRowIter(summs), nil
}

// evaluateArguments returns fromCommitVal, toCommitVal, dotCommitVal, and tableName.
// It evaluates the argument expressions to turn them into values this DiffSummaryTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (ds *SchemaDiffTableFunction) evaluateArguments() (interface{}, interface{}, interface{}, string, error) {
	var tableName string
	if ds.tableNameExpr != nil {
		tableNameVal, err := ds.tableNameExpr.Eval(ds.ctx, nil)
		if err != nil {
			return nil, nil, nil, "", err
		}
		tn, ok := tableNameVal.(string)
		if !ok {
			return nil, nil, nil, "", ErrInvalidTableName.New(ds.tableNameExpr.String())
		}
		tableName = tn
	}

	if ds.dotCommitExpr != nil {
		dotCommitVal, err := ds.dotCommitExpr.Eval(ds.ctx, nil)
		if err != nil {
			return nil, nil, nil, "", err
		}

		return nil, nil, dotCommitVal, tableName, nil
	}

	fromCommitVal, err := ds.fromCommitExpr.Eval(ds.ctx, nil)
	if err != nil {
		return nil, nil, nil, "", err
	}

	toCommitVal, err := ds.toCommitExpr.Eval(ds.ctx, nil)
	if err != nil {
		return nil, nil, nil, "", err
	}

	return fromCommitVal, toCommitVal, nil, tableName, nil
}

type schemaDiffTableFunctionRowIter struct {
	fromTableName string
	toTableName   string
	fromCreate    string
	toCreate      string
	fromHash      string
	toHash        string
	isDone        bool
}

func (s *schemaDiffTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if s.isDone {
		return nil, io.EOF
	} else {
		s.isDone = true
		return sql.NewRow(
			s.fromTableName, // 0
			s.toTableName,   // 1
			s.fromCreate,    // 2
			s.toCreate,      // 3
			s.fromHash,      // 4
			s.toHash,        // 5
		), nil
	}
}

func (s *schemaDiffTableFunctionRowIter) Close(context *sql.Context) error {
	return nil
}

var _ sql.RowIter = (*schemaDiffTableFunctionRowIter)(nil)
