// Copyright 2025 Dolthub, Inc.
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
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"io"
	"strings"
)

const testsRunDefaultRowCount = 10

var _ sql.TableFunction = (*TestsRunTableFunction)(nil)
var _ sql.CatalogTableFunction = (*TestsRunTableFunction)(nil)
var _ sql.ExecSourceRel = (*TestsRunTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*TestsRunTableFunction)(nil)

type testResult struct {
	groupName string
	testName  string
	query     string
	status    string
	error     string
}

type TestsRunTableFunction struct {
	ctx      *sql.Context
	catalog  sql.Catalog
	database sql.Database
	argument sql.Expression
	engine   *gms.Engine
	results  []testResult
}

var testRunTableSchema = sql.Schema{
	&sql.Column{Name: "test_group_name", Type: types.Text},
	&sql.Column{Name: "test_name", Type: types.Text},
	&sql.Column{Name: "query", Type: types.Text},
	&sql.Column{Name: "status", Type: types.Text},
	&sql.Column{Name: "error", Type: types.Text},
}

func (trtf *TestsRunTableFunction) NewInstance(ctx *sql.Context, database sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &TestsRunTableFunction{
		ctx:      ctx,
		database: database,
	}
	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// WithCatalog implements the sql.CatalogTableFunction interface
func (trtf *TestsRunTableFunction) WithCatalog(c sql.Catalog) (sql.TableFunction, error) {
	newInstance := *trtf
	newInstance.catalog = c
	pro, ok := c.(sql.DatabaseProvider)
	if !ok {
		return nil, fmt.Errorf("unable to get database provider")
	}
	newInstance.engine = gms.NewDefault(pro)

	err := newInstance.queryAndAssert()
	if err != nil {
		return nil, err
	}

	return &newInstance, nil
}

func (trtf *TestsRunTableFunction) queryAndAssert() error {
	tableIter, err := trtf.getDoltTestsData()
	if err != nil {
		return err
	}

	for {
		row, err := tableIter.Next(trtf.ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		var failMsg string

		query, err := actions.GetStringColAsString(trtf.ctx, row[2])
		if err != nil {
			return err
		}
		assertion, err := actions.GetStringColAsString(trtf.ctx, row[3])
		if err != nil {
			return err
		}

		if !strings.Contains(strings.ToLower(query), "select") && !strings.Contains(strings.ToLower(query), "show") {
			failMsg = "Cannot execute write queries"
		} else {
			_, queryResult, _, err := trtf.engine.Query(trtf.ctx, query)
			if err != nil {
				failMsg = fmt.Sprintf("Query error: %s", err.Error())
			} else {
				failMsg, err = actions.AssertData(trtf.ctx, assertion, &queryResult)
				if err != nil {
					return err
				}
			}
		}

		var result testResult
		result.groupName = row[0].(string)
		result.testName = row[1].(string)
		result.query = query
		result.status = "PASS"
		if failMsg != "" {
			result.status = "FAIL"
			result.error = failMsg
		}

		trtf.results = append(trtf.results, result)
	}
	return nil
}

func (trtf *TestsRunTableFunction) getDoltTestsData() (sql.RowIter, error) {
	testName := strings.ReplaceAll(trtf.argument.String(), "'", "")
	bv, err := sqltypes.BuildBindVariable(testName)
	if err != nil {
		return nil, err
	}
	value, err := sqltypes.BindVariableToValue(bv)
	if err != nil {
		return nil, err
	}
	expr, err := sqlparser.ExprFromValue(value)
	if err != nil {
		return nil, err
	}

	bindingsMap := make(map[string]sqlparser.Expr)
	bindingsMap["v1"] = expr

	qry := fmt.Sprintf("SELECT * FROM dolt_tests WHERE test_group = ?")
	_, iter, _, err := trtf.engine.QueryWithBindings(trtf.ctx, qry, nil, bindingsMap, nil)
	if err != nil {
		return nil, err
	}

	return iter, nil
}

// Database implements the sql.Databaser interface
func (trtf *TestsRunTableFunction) Database() sql.Database {
	return trtf.database
}

// WithDatabase impelement the sql.Databaser interface
func (trtf *TestsRunTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	ntf := *trtf
	ntf.database = database
	return &ntf, nil
}

// Expressions implements the sql.Expressioner interface
func (trtf *TestsRunTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{trtf.argument}
}

// WithExpressions implements the sql.Expressioner interface
func (trtf *TestsRunTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(trtf.Name(), "1", len(expression))
	}

	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(trtf.Name(), expr.String())
		}

		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(trtf.Name(), expr.String())
		}
	}

	newTrtf := *trtf
	newTrtf.argument = expression[0]

	return &newTrtf, nil
}

// Children implements the sql.Node interface
func (trtf *TestsRunTableFunction) Children() []sql.Node {
	return nil
}

func (trtf *TestsRunTableFunction) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return trtf, nil
}

// CheckAuth implements the interface sql.AuthorizationCheckerNode
func (trtf *TestsRunTableFunction) CheckAuth(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	subject := sql.PrivilegeCheckSubject{Database: trtf.database.Name()}
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
}

// Schema implements the sql.Node interface
func (trtf *TestsRunTableFunction) Schema() sql.Schema {
	return testRunTableSchema
}

// Resolved implements the sql.Resolvable interface
func (trtf *TestsRunTableFunction) Resolved() bool {
	return trtf.argument.Resolved()
}

func (trtf *TestsRunTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (trtf *TestsRunTableFunction) String() string {
	return fmt.Sprintf("DOLT_TEST_RUN('%s')", trtf.argument.String())
}

// Name impelements the sql.TableFunction interface
func (trtf *TestsRunTableFunction) Name() string {
	return "dolt_test_run"
}

// RowIter implements the sql.Node interface
func (trtf *TestsRunTableFunction) RowIter(_ *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if len(trtf.results) == 0 {
		return nil, fmt.Errorf("could not find tests for test group with name: %s", trtf.argument.String())
	}

	var rows []sql.Row
	for _, result := range trtf.results {
		newRow := sql.NewRow(result.groupName, result.testName, result.query, result.status, result.error)
		rows = append(rows, newRow)
	}

	return sql.RowsToRowIter(rows...), nil
}

// DataLength estimates total data size for query planning.
func (trtf *TestsRunTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(trtf.Schema())
	numRows, _, err := trtf.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (trtf *TestsRunTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return testsRunDefaultRowCount, false, nil
}
