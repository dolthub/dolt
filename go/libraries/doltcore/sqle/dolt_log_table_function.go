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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var _ sql.TableFunction = (*LogTableFunction)(nil)

type LogTableFunction struct {
	ctx *sql.Context

	revisionExpr       sql.Expression
	secondRevisionExpr sql.Expression
	database           sql.Database
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
	if ltf.revisionExpr != nil && ltf.secondRevisionExpr != nil {
		return ltf.revisionExpr.Resolved() && ltf.secondRevisionExpr.Resolved()
	}
	if ltf.revisionExpr != nil {
		return ltf.revisionExpr.Resolved()
	}
	return true
}

// String implements the Stringer interface
func (ltf *LogTableFunction) String() string {
	if ltf.revisionExpr != nil && ltf.secondRevisionExpr != nil {
		return fmt.Sprintf("DOLT_LOG(%s, %s)", ltf.revisionExpr.String(), ltf.secondRevisionExpr.String())
	}
	if ltf.revisionExpr != nil {
		return fmt.Sprintf("DOLT_LOG(%s)", ltf.revisionExpr.String())
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
	if ltf.revisionExpr != nil {
		exprs = append(exprs, ltf.revisionExpr)
	}
	if ltf.secondRevisionExpr != nil {
		exprs = append(exprs, ltf.secondRevisionExpr)
	}
	return exprs
}

// WithExpressions implements the sql.Expressioner interface.
func (ltf *LogTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) > 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(ltf.FunctionName(), "0 to 2", len(expression))
	}

	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(ltf.FunctionName(), expr.String())
		}
	}

	exLen := len(expression)
	if exLen > 0 {
		ltf.revisionExpr = expression[0]
	}
	if exLen == 2 {
		ltf.secondRevisionExpr = expression[1]
	}

	if err := ltf.validateRevisionExpressions(); err != nil {
		return nil, err
	}

	return ltf, nil
}

func (ltf *LogTableFunction) invalidArgDetailsErr(expr sql.Expression, reason string) *errors.Error {
	return sql.ErrInvalidArgumentDetails.New(ltf.FunctionName(), fmt.Sprintf("%s - %s", expr.String(), reason))
}

func (ltf *LogTableFunction) validateRevisionExpressions() error {
	if ltf.revisionExpr != nil {
		if !sql.IsText(ltf.revisionExpr.Type()) {
			return sql.ErrInvalidArgumentDetails.New(ltf.FunctionName(), ltf.revisionExpr.String())
		}
		if ltf.secondRevisionExpr == nil && strings.Contains(ltf.revisionExpr.String(), "^") {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "second revision must exist if first revision contains '^'")
		}
		if strings.Contains(ltf.revisionExpr.String(), "..") && strings.Contains(ltf.revisionExpr.String(), "^") {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "revision cannot contain both '..' and '^'")
		}
	}

	if ltf.secondRevisionExpr != nil {
		if !sql.IsText(ltf.secondRevisionExpr.Type()) {
			return sql.ErrInvalidArgumentDetails.New(ltf.FunctionName(), ltf.secondRevisionExpr.String())
		}
		if strings.Contains(ltf.secondRevisionExpr.String(), "..") {
			return ltf.invalidArgDetailsErr(ltf.secondRevisionExpr, "second revision cannot contain '..'")
		}
		if strings.Contains(ltf.revisionExpr.String(), "..") {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "revision cannot contain '..' if second revision exists")
		}
	}

	if ltf.revisionExpr != nil && ltf.secondRevisionExpr != nil {
		if strings.Contains(ltf.revisionExpr.String(), "^") && strings.Contains(ltf.secondRevisionExpr.String(), "^") {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "both revisions cannot contain '^'")
		}
		if !strings.Contains(ltf.revisionExpr.String(), "^") && !strings.Contains(ltf.secondRevisionExpr.String(), "^") {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "one revision must contain '^' if two revisions provided")
		}
	}

	return nil
}

// RowIter implements the sql.Node interface
func (ltf *LogTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	revisionVal, excludingRevisionVal, err := ltf.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := ltf.database.(Database)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", ltf.database)
	}

	sess := dsess.DSessFromSess(ctx.Session)
	var commit *doltdb.Commit

	if len(revisionVal) > 0 {
		cs, err := doltdb.NewCommitSpec(revisionVal)
		if err != nil {
			return nil, err
		}

		commit, err = sqledb.ddb.Resolve(ctx, cs, nil)
		if err != nil {
			return nil, err
		}
	} else {
		// If revisionExpr not defined, use session head
		commit, err = sess.GetHeadCommit(ctx, sqledb.name)
		if err != nil {
			return nil, err
		}
	}

	// Two dot log
	if len(excludingRevisionVal) > 0 {
		exCs, err := doltdb.NewCommitSpec(excludingRevisionVal)
		if err != nil {
			return nil, err
		}

		excludingCommit, err := sqledb.ddb.Resolve(ctx, exCs, nil)
		if err != nil {
			return nil, err
		}
		return NewDotDotLogTableFunctionRowIter(ctx, sqledb.ddb, commit, excludingCommit)
	}

	return NewLogTableFunctionRowIter(ctx, sqledb.ddb, commit)
}

// evaluateArguments returns revisionValStr and excludingRevisionValStr.
// It evaluates the argument expressions to turn them into values this LogTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (ltf *LogTableFunction) evaluateArguments() (string, string, error) {
	var revisionValStr string
	var excludingRevisionValStr string
	var err error

	if ltf.revisionExpr != nil {
		revisionValStr, excludingRevisionValStr, err = getRevisionsFromExpr(ltf.ctx, ltf.revisionExpr, true)
		if err != nil {
			return "", "", err
		}
	}

	if ltf.secondRevisionExpr != nil {
		rvs, ervs, err := getRevisionsFromExpr(ltf.ctx, ltf.secondRevisionExpr, false)
		if err != nil {
			return "", "", err
		}
		if len(rvs) > 0 {
			revisionValStr = rvs
		}
		if len(ervs) > 0 {
			excludingRevisionValStr = ervs
		}
	}

	return revisionValStr, excludingRevisionValStr, nil
}

// Gets revisionName and/or excludingRevisionName from sql expression
func getRevisionsFromExpr(ctx *sql.Context, expr sql.Expression, canDot bool) (string, string, error) {
	revisionVal, err := expr.Eval(ctx, nil)
	if err != nil {
		return "", "", err
	}

	revisionValStr, ok := revisionVal.(string)
	if !ok {
		return "", "", fmt.Errorf("received '%v' when expecting revision string", revisionVal)
	}

	if canDot && strings.Contains(revisionValStr, "..") {
		refs := strings.Split(revisionValStr, "..")
		return refs[1], refs[0], nil
	}

	if strings.Contains(revisionValStr, "^") {
		return "", strings.TrimPrefix(revisionValStr, "^"), nil
	}

	return revisionValStr, "", nil
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

func NewDotDotLogTableFunctionRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, commit, excludingCommit *doltdb.Commit) (*logTableFunctionRowIter, error) {
	hash, err := commit.HashOf()
	if err != nil {
		return nil, err
	}

	exHash, err := excludingCommit.HashOf()
	if err != nil {
		return nil, err
	}

	child, err := commitwalk.GetDotDotRevisionsIterator(ctx, ddb, hash, exHash)
	if err != nil {
		return nil, err
	}

	return &logTableFunctionRowIter{child}, nil
}
