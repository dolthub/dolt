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
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
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
	testName  string
	groupName string
	query     string
	status    string
	message   string
}

type TestsRunTableFunction struct {
	ctx           *sql.Context
	catalog       sql.Catalog
	database      sql.Database
	argumentExprs []sql.Expression
	engine        *gms.Engine
}

var testRunTableSchema = sql.Schema{
	&sql.Column{Name: "test_name", Type: types.Text},
	&sql.Column{Name: "test_group_name", Type: types.Text},
	&sql.Column{Name: "query", Type: types.Text},
	&sql.Column{Name: "status", Type: types.Text},
	&sql.Column{Name: "message", Type: types.Text},
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

	return &newInstance, nil
}

// Database implements the sql.Databaser interface
func (trtf *TestsRunTableFunction) Database() sql.Database {
	return trtf.database
}

// WithDatabase implements the sql.Databaser interface
func (trtf *TestsRunTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	ntf := *trtf
	ntf.database = database
	return &ntf, nil
}

// Expressions implements the sql.Expressioner interface
func (trtf *TestsRunTableFunction) Expressions() []sql.Expression {
	return trtf.argumentExprs
}

// WithExpressions implements the sql.Expressioner interface
func (trtf *TestsRunTableFunction) WithExpressions(expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) < 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(trtf.Name(), "1 or more", len(expressions))
	}

	for _, expr := range expressions {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(trtf.Name(), expr.String())
		}

		// We don't allow functions as arguments to dolt_test_run
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(trtf.Name(), expr.String())
		}
	}

	newTrtf := *trtf
	newTrtf.argumentExprs = expressions
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
	for _, expr := range trtf.argumentExprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (trtf *TestsRunTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (trtf *TestsRunTableFunction) String() string {
	return fmt.Sprintf("DOLT_TEST_RUN(%s)", trtf.getOptionsString())
}

// getOptionsString builds comma-separated argument list for display.
func (trtf *TestsRunTableFunction) getOptionsString() string {
	var options []string
	for _, expr := range trtf.argumentExprs {
		options = append(options, expr.String())
	}

	return strings.Join(options, ",")
}

// Name implements the sql.TableFunction interface
func (trtf *TestsRunTableFunction) Name() string {
	return "dolt_test_run"
}

// RowIter implements the sql.Node interface
func (trtf *TestsRunTableFunction) RowIter(_ *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var rows []sql.Row

	for _, expr := range trtf.argumentExprs {
		toTest := strings.Trim(expr.String(), "'")
		tableIter, err := trtf.getDoltTestsData(toTest)
		if err != nil {
			return nil, err
		}

		for {
			row, err := tableIter.Next(trtf.ctx)
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}

			result, err := trtf.queryAndAssert(row)
			if err != nil {
				return nil, err
			}

			newRow := sql.NewRow(result.testName, result.groupName, result.query, result.status, result.message)
			rows = append(rows, newRow)
		}
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

func (trtf *TestsRunTableFunction) queryAndAssert(row sql.Row) (result testResult, err error) {
	testName, groupName, query, assertion, comparison, value, err := parseDoltTestsRow(trtf.ctx, row)
	if err != nil {
		return
	}
	message, err := validateQuery(trtf.ctx, trtf.catalog, query)
	if err != nil {
		return
	}

	var testPassed bool
	if message == "" {
		_, queryResult, _, err := trtf.engine.Query(trtf.ctx, query)
		if err != nil {
			message = fmt.Sprintf("Query error: %s", err.Error())
		} else {
			testPassed, message, err = actions.AssertData(trtf.ctx, assertion, comparison, value, &queryResult)
			if err != nil {
				return testResult{}, err
			}
		}
	}

	status := "PASS"
	if !testPassed {
		status = "FAIL"
	}
	result = testResult{testName, groupName, query, status, message}
	return result, nil
}

func (trtf *TestsRunTableFunction) getDoltTestsData(toTest string) (sql.RowIter, error) {
	testData := strings.Split(toTest, " ")
	if len(testData) == 0 {
		return nil, fmt.Errorf("invalid inputs")
	}

	var qry string
	switch testData[0] {
	case "*":
		qry = "SELECT * FROM dolt_tests"
		if len(testData) != 1 {
			return nil, fmt.Errorf("invalid inputs to dolt_test_run: %s", toTest)
		}
	case "test":
		if len(testData) < 2 {
			return nil, fmt.Errorf("invalid inputs to dolt_test_run: %s", toTest)
		}
		qry = "SELECT * FROM dolt_tests WHERE test_name = ?"
	case "group":
		if len(testData) < 2 {
			return nil, fmt.Errorf("invalid inputs to dolt_test_run: %s", toTest)
		}
		qry = "SELECT * FROM dolt_tests WHERE test_group = ?"
	default:
		return nil, fmt.Errorf("invalid input to dolt_test_run: %s", toTest)
	}

	// If we're using all tests, we can just query the table broadly
	if testData[0] == "*" {
		_, iter, _, err := trtf.engine.Query(trtf.ctx, qry)
		if err != nil {
			return nil, err
		}
		return iter, nil
	}

	// Otherwise we need to bind the query
	testName := strings.Join(testData[1:], " ")
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

	_, iter, _, err := trtf.engine.QueryWithBindings(trtf.ctx, qry, nil, bindingsMap, nil)
	if err != nil {
		return nil, err
	}

	return iter, nil
}

func IsWriteQuery(query string, ctx *sql.Context, catalog sql.Catalog) (bool, error) {
	builder := planbuilder.New(ctx, catalog, nil, nil)

	parsed, _, _, err := sql.GlobalParser.Parse(ctx, query, false)
	if err != nil {
		return false, err
	}

	node, _, err := builder.BindOnly(parsed, query, nil)
	if err != nil {
		return false, err
	}
	return !node.IsReadOnly(), nil
}

func parseDoltTestsRow(ctx *sql.Context, row sql.Row) (testName, groupName, query, assertion, comparison, value string, err error) {
	if testName, err = actions.GetStringColAsString(ctx, row[0]); err != nil {
		return
	}
	if groupName, err = actions.GetStringColAsString(ctx, row[1]); err != nil {
		return
	}
	if query, err = actions.GetStringColAsString(ctx, row[2]); err != nil {
		return
	}
	if assertion, err = actions.GetStringColAsString(ctx, row[3]); err != nil {
		return
	}
	if comparison, err = actions.GetStringColAsString(ctx, row[4]); err != nil {
		return
	}
	if value, err = actions.GetStringColAsString(ctx, row[5]); err != nil {
		return
	}

	return testName, groupName, query, assertion, comparison, value, nil
}

func validateQuery(ctx *sql.Context, catalog sql.Catalog, query string) (string, error) {
	// We first check if the query contains multiple sql statements
	if statements, err := sqlparser.SplitStatementToPieces(query); err != nil {
		return "", err
	} else if len(statements) > 1 {
		return "Cannot execute multiple queries", nil
	}

	if isWrite, err := IsWriteQuery(query, ctx, catalog); err != nil {
		return "", err
	} else if isWrite {
		return "Cannot execute write queries", nil
	}
	return "", nil
}
