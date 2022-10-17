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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.TableFunction = (*LogTableFunction)(nil)

type LogTableFunction struct {
	ctx *sql.Context

	commitExpr sql.Expression
	database   sql.Database
}

var logTableSchema = sql.Schema{
	&sql.Column{Name: "commit_hash", Type: sql.Text},
	&sql.Column{Name: "committer", Type: sql.Text},
	&sql.Column{Name: "email", Type: sql.Text},
	&sql.Column{Name: "date", Type: sql.Datetime},
	&sql.Column{Name: "message", Type: sql.Text},
}

// NewInstance creates a new instance of TableFunction interface
func (ltf *LogTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &LogTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Database implements the sql.Databaser interface
func (ltf *LogTableFunction) Database() sql.Database {
	return ltf.database
}

// WithDatabase implements the sql.Databaser interface
func (ltf *LogTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	ltf.database = database
	return ltf, nil
}

// FunctionName implements the sql.TableFunction interface
func (ltf *LogTableFunction) FunctionName() string {
	return "dolt_log"
}

// Resolved implements the sql.Resolvable interface
func (ltf *LogTableFunction) Resolved() bool {
	if ltf.commitExpr != nil {
		return ltf.commitExpr.Resolved()
	}
	return true
}

// String implements the Stringer interface
func (ltf *LogTableFunction) String() string {
	if ltf.commitExpr != nil {
		return fmt.Sprintf("DOLT_LOG(%s)", ltf.commitExpr.String())
	}
	return "DOLT_LOG()"
}

// Schema implements the sql.Node interface.
func (ltf *LogTableFunction) Schema() sql.Schema {
	return logTableSchema
}

// Children implements the sql.Node interface.
func (ltf *LogTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (ltf *LogTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return ltf, nil
}

// CheckPrivileges implements the interface sql.Node.
func (ltf *LogTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	tblNames, err := ltf.database.GetTableNames(ctx)
	if err != nil {
		return false
	}

	var operations []sql.PrivilegedOperation
	for _, tblName := range tblNames {
		operations = append(operations, sql.NewPrivilegedOperation(ltf.database.Name(), tblName, "", sql.PrivilegeType_Select))
	}

	return opChecker.UserHasPrivileges(ctx, operations...)
}

// Expressions implements the sql.Expressioner interface.
func (ltf *LogTableFunction) Expressions() []sql.Expression {
	exprs := []sql.Expression{}
	if ltf.commitExpr != nil {
		exprs = append(exprs, ltf.commitExpr)
	}
	return exprs
}

// WithExpressions implements the sql.Expressioner interface.
func (ltf *LogTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) < 0 || len(expression) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(ltf.FunctionName(), "0 or 1", len(expression))
	}

	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(ltf.FunctionName(), expr.String())
		}
	}

	exLen := len(expression)
	if exLen == 1 {
		ltf.commitExpr = expression[0]
	}

	// validate the expressions
	if ltf.commitExpr != nil {
		if !sql.IsText(ltf.commitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(ltf.FunctionName(), ltf.commitExpr.String())
		}
	}

	return ltf, nil
}

// RowIter implements the sql.Node interface
func (ltf *LogTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	commitVal, err := ltf.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := ltf.database.(Database)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", ltf.database)
	}

	sess := dsess.DSessFromSess(ctx.Session)
	var commit *doltdb.Commit

	if ltf.commitExpr != nil {
		cs, err := doltdb.NewCommitSpec(commitVal)
		if err != nil {
			return nil, err
		}

		commit, err = sqledb.GetDoltDB().Resolve(ctx, cs, nil)
	} else {
		// If commitExpr not defined, use session head
		commit, err = sess.GetHeadCommit(ctx, sqledb.name)
	}

	if err != nil {
		return nil, err
	}

	return NewLogTableFunctionRowIter(ctx, sqledb.GetDoltDB(), commit)
}

// evaluateArguments returns commitValStr.
// It evaluates the argument expressions to turn them into values this LogTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (ltf *LogTableFunction) evaluateArguments() (string, error) {
	if ltf.commitExpr != nil {
		commitVal, err := ltf.commitExpr.Eval(ltf.ctx, nil)
		if err != nil {
			return "", err
		}

		commitValStr, ok := commitVal.(string)
		if !ok {
			return "", fmt.Errorf("received '%v' when expecting commit hash string", commitVal)
		}

		return commitValStr, nil
	}

	return "", nil
}

//------------------------------------
// logTableFunctionRowIter
//------------------------------------

var _ sql.RowIter = (*logTableFunctionRowIter)(nil)

// logTableFunctionRowIter is a sql.RowIter implementation which iterates over each commit as if it's a row in the table.
type logTableFunctionRowIter struct {
	child doltdb.CommitItr
}

func NewLogTableFunctionRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit) (*logTableFunctionRowIter, error) {
	hash, err := commit.HashOf()
	if err != nil {
		return nil, err
	}

	child, err := commitwalk.GetTopologicalOrderIterator(ctx, ddb, hash)
	if err != nil {
		return nil, err
	}

	return &logTableFunctionRowIter{child}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *logTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	h, cm, err := itr.child.Next(ctx)
	if err != nil {
		return nil, err
	}

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewRow(h.String(), meta.Name, meta.Email, meta.Time(), meta.Description), nil
}

func (itr *logTableFunctionRowIter) Close(_ *sql.Context) error {
	return nil
}
