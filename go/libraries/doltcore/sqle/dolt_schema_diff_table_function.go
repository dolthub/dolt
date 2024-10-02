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
	"io"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
)

const schemaDiffDefaultRowCount = 100

var _ sql.TableFunction = (*SchemaDiffTableFunction)(nil)
var _ sql.ExecSourceRel = (*SchemaDiffTableFunction)(nil)

type SchemaDiffTableFunction struct {
	ctx *sql.Context

	// the below expressions are set when the function is invoked in a particular way

	// fromCommitExpr is the first expression, provided there are 2 or 3 expressions
	// dolt_schema_diff('from_commit', 'to_commit') -> dolt_schema_diff('123', '456')
	// dolt_schema_diff('from_commit', 'to_commit', 'table') -> dolt_schema_diff('123', '456', 'foo')
	fromCommitExpr sql.Expression

	// toCommitExpr is the second expression, provided there are 2 or 3 expressions
	// dolt_schema_diff('from_commit', 'to_commit') -> dolt_schema_diff('123', '456')
	// dolt_schema_diff('from_commit', 'to_commit', 'table_name') -> dolt_schema_diff('123', '456', 'foo')
	toCommitExpr sql.Expression

	// dotCommitExpr is the first expression, provided there are 1 or 2 expressions
	// dolt_schema_diff('dot_commit') -> dolt_schema_diff('HEAD^..HEAD')
	// dolt_schema_diff('dot_commit', 'table_name') -> dolt_schema_diff('HEAD^..HEAD', 'foo')
	dotCommitExpr sql.Expression

	// tableNameExpr is the expression that follows the commit expressions
	// dolt_schema_diff('from_commit', 'to_commit', 'table_name') -> dolt_schema_diff('123', '456', 'my_table')
	// dolt_schema_diff('dot_commit', 'table_name') -> dolt_schema_diff('123..456', 'my_table')
	tableNameExpr sql.Expression

	database sql.Database
}

var schemaDiffTableSchema = sql.Schema{
	&sql.Column{Name: "from_table_name", Type: types.LongText, Nullable: false},   // 0
	&sql.Column{Name: "to_table_name", Type: types.LongText, Nullable: false},     // 1
	&sql.Column{Name: "from_create_statement", Type: types.Text, Nullable: false}, // 2
	&sql.Column{Name: "to_create_statement", Type: types.Text, Nullable: false},   // 3
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

	return node, nil
}

func (ds *SchemaDiffTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(ds.Schema())
	numRows, _, err := ds.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (ds *SchemaDiffTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return schemaDiffDefaultRowCount, false, nil
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

func (ds *SchemaDiffTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (ds *SchemaDiffTableFunction) String() string {
	if ds.dotCommitExpr != nil {
		if ds.tableNameExpr != nil {
			return fmt.Sprintf("DOLT_SCHEMA_DIFF(%s, %s)", ds.dotCommitExpr.String(), ds.tableNameExpr.String())
		} else {
			return fmt.Sprintf("DOLT_SCHEMA_DIFF(%s)", ds.dotCommitExpr.String())
		}
	}
	if ds.tableNameExpr != nil {
		return fmt.Sprintf("DOLT_SCHEMA_DIFF(%s, %s, %s)", ds.fromCommitExpr.String(), ds.toCommitExpr.String(), ds.tableNameExpr.String())
	} else {
		return fmt.Sprintf("DOLT_SCHEMA_DIFF(%s, %s)", ds.fromCommitExpr.String(), ds.toCommitExpr.String())
	}
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
	if ds.tableNameExpr != nil {
		_, _, _, tableName, err := ds.evaluateArguments()
		if err != nil {
			return false
		}

		subject := sql.PrivilegeCheckSubject{Database: ds.database.Name(), Table: tableName}
		return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
	}

	tblNames, err := ds.database.GetTableNames(ctx)
	if err != nil {
		return false
	}
	operations := make([]sql.PrivilegedOperation, 0, len(tblNames))
	for _, tblName := range tblNames {
		subject := sql.PrivilegeCheckSubject{Database: ds.database.Name(), Table: tblName}
		operations = append(operations, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
	}

	return opChecker.UserHasPrivileges(ctx, operations...)
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
func (ds *SchemaDiffTableFunction) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) < 1 || len(exprs) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New(ds.Name(), "1 to 3", len(exprs))
	}

	for _, expr := range exprs {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(ds.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(ds.Name(), expr.String())
		}
	}

	newDstf := *ds
	if strings.Contains(exprs[0].String(), "..") {
		newDstf.dotCommitExpr = exprs[0]
		if len(exprs) > 1 {
			newDstf.tableNameExpr = exprs[1]
		}
	} else {
		if len(exprs) < 2 {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), "There are less than 2 arguments present, and the first does not contain '..'")
		}
		newDstf.fromCommitExpr = exprs[0]
		newDstf.toCommitExpr = exprs[1]
		if len(exprs) > 2 {
			newDstf.tableNameExpr = exprs[2]
		}
	}

	// validate the expressions
	if newDstf.dotCommitExpr != nil {
		if !types.IsText(newDstf.dotCommitExpr.Type()) && !expression.IsBindVar(newDstf.dotCommitExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.dotCommitExpr.String())
		}
	} else {
		if !types.IsText(newDstf.fromCommitExpr.Type()) && !expression.IsBindVar(newDstf.fromCommitExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.fromCommitExpr.String())
		}
		if !types.IsText(newDstf.toCommitExpr.Type()) && !expression.IsBindVar(newDstf.toCommitExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.toCommitExpr.String())
		}
	}

	if newDstf.tableNameExpr != nil && !types.IsText(newDstf.tableNameExpr.Type()) && !expression.IsBindVar(newDstf.tableNameExpr) {
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

	deltas, err := diff.GetTableDeltas(ctx, fromRoot, toRoot)
	if err != nil {
		return nil, err
	}

	sort.Slice(deltas, func(i, j int) bool {
		return deltas[i].ToName.Less(deltas[j].ToName)
	})

	dataRows := []sql.Row{}
	for _, delta := range deltas {
		// TODO: schema name
		shouldInclude := tableName == "" || tableName == delta.ToName.Name || tableName == delta.FromName.Name
		if !shouldInclude {
			continue
		}

		fromName := delta.FromName
		toName := delta.ToName

		var fromCreate, toCreate string

		if delta.FromTable != nil {
			fromCreate, err = sqlfmt.GenerateCreateTableStatement(delta.FromName.Name, delta.FromSch, delta.FromFks, delta.FromFksParentSch)
			if err != nil {
				return nil, err
			}
		}

		if delta.ToTable != nil {
			toCreate, err = sqlfmt.GenerateCreateTableStatement(delta.ToName.Name, delta.ToSch, delta.ToFks, delta.ToFksParentSch)
			if err != nil {
				return nil, err
			}
		}

		isDbCollationDiff := strings.HasPrefix(fromName.Name, diff.DBPrefix) || strings.HasPrefix(toName.Name, diff.DBPrefix)
		var schemasAreDifferent = fromCreate != toCreate || isDbCollationDiff
		if !schemasAreDifferent {
			continue
		}

		row := sql.Row{
			fromName.String(), // from_table_name
			toName.String(),   // to_table_name
			fromCreate,        // from_create_statement
			toCreate,          // to_create_statement
		}
		dataRows = append(dataRows, row)
	}

	iter := &schemaDiffTableFunctionRowIter{
		rows: dataRows,
		idx:  0,
	}

	return iter, nil
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
	rows []sql.Row
	idx  int
}

func (s *schemaDiffTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if s.idx >= len(s.rows) {
		return nil, io.EOF
	} else {
		row := s.rows[s.idx]
		s.idx++
		return row, nil
	}
}

func (s *schemaDiffTableFunctionRowIter) Close(context *sql.Context) error {
	return nil
}

var _ sql.RowIter = (*schemaDiffTableFunctionRowIter)(nil)
