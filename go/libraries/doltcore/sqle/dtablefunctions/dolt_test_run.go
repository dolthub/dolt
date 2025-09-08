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
	"strings"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
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

func (trtf *TestsRunTableFunction) NewInstance(_ *sql.Context, database sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &TestsRunTableFunction{
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
	return fmt.Sprintf("DOLT_TEST_RUN(%s)", strings.Join(trtf.getOptionsString(), ","))
}

// getOptionsString builds a slice of the arguments passed into dolt_test_run
func (trtf *TestsRunTableFunction) getOptionsString() []string {
	var options []string
	for _, expr := range trtf.argumentExprs {
		options = append(options, expr.String())
	}
	return options
}

// Name implements the sql.TableFunction interface
func (trtf *TestsRunTableFunction) Name() string {
	return "dolt_test_run"
}

// RowIter implements the sql.Node interface
func (trtf *TestsRunTableFunction) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	args := trtf.getOptionsString()
	if len(args) == 0 { // We treat no arguments as a wildcard
		args = append(args, "*")
	}

	var resultRows []sql.Row
	for _, arg := range args {
		testRows, err := func() (*[]sql.Row, error) {
			ctx, cancel := ctx.NewSubContext()
			defer cancel()

			return trtf.getDoltTestsData(ctx, strings.Trim(arg, "'"))
		}()
		if err != nil {
			return nil, err
		}

		for _, row := range *testRows {
			result, err := func() (testResult, error) {
				ctx, cancel := ctx.NewSubContext()
				defer cancel()
				return trtf.queryAndAssert(ctx, row)
			}()
			if err != nil {
				return nil, err
			}

			resultRow := sql.NewRow(result.testName, result.groupName, result.query, result.status, result.message)
			resultRows = append(resultRows, resultRow)
		}
	}
	return sql.RowsToRowIter(resultRows...), nil
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

func (trtf *TestsRunTableFunction) queryAndAssert(ctx *sql.Context, row sql.Row) (result testResult, err error) {
	testName, groupName, query, assertion, comparison, value, err := parseDoltTestsRow(ctx, row)
	if err != nil {
		return
	}

	message, err := validateQuery(ctx, trtf.catalog, *query)
	if err != nil && message == "" {
		message = fmt.Sprintf("query error: %s", err.Error())
	}

	var testPassed bool
	if message == "" {
		queryResult, err := func() (sql.RowIter, error) {
			ctx, cancel := ctx.NewSubContext()
			defer cancel()
			_, queryResult, _, err := trtf.engine.Query(ctx, *query)
			return queryResult, err
		}()
		if err != nil {
			message = fmt.Sprintf("Query error: %s", err.Error())
		} else {
			testPassed, message, err = actions.AssertData(ctx, *assertion, *comparison, value, &queryResult)
			if err != nil {
				return testResult{}, err
			}
		}
	}

	status := "PASS"
	if !testPassed {
		status = "FAIL"
	}

	var groupString string
	if groupName != nil {
		groupString = *groupName
	}
	result = testResult{*testName, groupString, *query, status, message}
	return result, nil
}

func (trtf *TestsRunTableFunction) getDoltTestsData(ctx *sql.Context, arg string) (*[]sql.Row, error) {
	var queries []string

	if arg == "*" {
		queries = []string{
			"SELECT * FROM dolt_tests",
		}
	} else {
		getIndividual, err := dbr.InterpolateForDialect("SELECT * FROM dolt_tests WHERE test_name = ?", []interface{}{arg}, dialect.MySQL)
		if err != nil {
			return nil, err
		}
		getGroup, err := dbr.InterpolateForDialect("SELECT * FROM dolt_tests WHERE test_group = ?", []interface{}{arg}, dialect.MySQL)
		if err != nil {
			return nil, err
		}
		queries = []string{
			getIndividual, getGroup,
		}
	}

	for _, query := range queries {
		iter, err := func() (sql.RowIter, error) {
			ctx, cancel := ctx.NewSubContext()
			defer cancel()
			_, iter, _, err := trtf.engine.Query(ctx, query)
			return iter, err
		}()
		if err != nil {
			return nil, err
		}

		rows, err := sql.RowIterToRows(ctx, iter)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 {
			return &rows, nil
		}
	}
	return nil, fmt.Errorf("could not find tests for argument: %s", arg)
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

func parseDoltTestsRow(ctx *sql.Context, row sql.Row) (testName, groupName, query, assertion, comparison, value *string, err error) {
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
	} else if len(statements) != 1 {
		return "Can only run exactly one query", nil
	}

	if isWrite, err := IsWriteQuery(query, ctx, catalog); err != nil {
		return "", err
	} else if isWrite {
		return "Cannot execute write queries", nil
	}

	if strings.Contains(strings.ToLower(query), "dolt_test_run(") {
		return "Cannot call dolt_test_run in dolt_tests", nil
	}
	return "", nil
}
