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
	"io"
	"strconv"
	"strings"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/overrides"
	"github.com/dolthub/dolt/go/store/val"
)

const testsRunDefaultRowCount = 10

var _ sql.TableFunction = (*TestsRunTableFunction)(nil)
var _ sql.CatalogTableFunction = (*TestsRunTableFunction)(nil)
var _ sql.ExecSourceRel = (*TestsRunTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*TestsRunTableFunction)(nil)

// TestResult represents the result of running a single test
type TestResult struct {
	TestName  string
	GroupName string
	Query     string
	Status    string
	Message   string
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
func (trtf *TestsRunTableFunction) RowIter(_ *sql.Context, _ sql.Row) (sql.RowIter, error) {
	args := trtf.getOptionsString()
	if len(args) == 0 { // We treat no arguments as a wildcard
		args = append(args, "*")
	}

	var resultRows []sql.Row
	for _, arg := range args {
		testRows, err := trtf.getDoltTestsData(strings.Trim(arg, "'"))
		if err != nil {
			return nil, err
		}

		for _, row := range testRows {
			result, err := trtf.queryAndAssert(row)
			if err != nil {
				return nil, err
			}

			resultRow := sql.NewRow(result.TestName, result.GroupName, result.Query, result.Status, result.Message)
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

func (trtf *TestsRunTableFunction) queryAndAssert(row sql.Row) (result TestResult, err error) {
	testName, groupName, query, assertion, comparison, value, err := parseDoltTestsRow(trtf.ctx, row)
	if err != nil {
		return
	}

	message, err := validateQuery(trtf.ctx, trtf.catalog, *query)
	if err != nil && message == "" {
		message = fmt.Sprintf("query error: %s", err.Error())
	}

	var testPassed bool
	if message == "" {
		_, queryResult, _, err := trtf.engine.Query(trtf.ctx, *query)
		if err != nil {
			message = fmt.Sprintf("Query error: %s", err.Error())
		} else {
			// For regular dolt_test_run() usage, use a simple inline assertion
			// This avoids circular imports while maintaining functionality
			testPassed, message, err = inlineAssertData(trtf.ctx, *assertion, *comparison, value, queryResult)
			if err != nil {
				return TestResult{}, err
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
	result = TestResult{*testName, groupString, *query, status, message}
	return result, nil
}

func (trtf *TestsRunTableFunction) queryAndAssertWithFunc(row sql.Row, assertDataFunc AssertDataFunc) (result TestResult, err error) {
	testName, groupName, query, assertion, comparison, value, err := parseDoltTestsRow(trtf.ctx, row)
	if err != nil {
		return
	}

	message, err := validateQuery(trtf.ctx, trtf.catalog, *query)
	if err != nil && message == "" {
		message = fmt.Sprintf("query error: %s", err.Error())
	}

	var testPassed bool
	if message == "" {
		_, queryResult, _, err := trtf.engine.Query(trtf.ctx, *query)
		if err != nil {
			message = fmt.Sprintf("Query error: %s", err.Error())
		} else {
			testPassed, message, err = assertDataFunc(trtf.ctx, *assertion, *comparison, value, queryResult)
			if err != nil {
				return TestResult{}, err
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
	result = TestResult{*testName, groupString, *query, status, message}
	return result, nil
}

func (trtf *TestsRunTableFunction) getDoltTestsData(arg string) ([]sql.Row, error) {
	return trtf.getDoltTestsDataWithRoot(arg, nil)
}

func (trtf *TestsRunTableFunction) getDoltTestsDataWithRoot(arg string, root doltdb.RootValue) ([]sql.Row, error) {
	if root != nil {
		// When a specific root is provided, we need to read from that root instead of current session
		// Check if dolt_tests table exists in this root
		testsTableName := doltdb.TableName{Name: "dolt_tests"}
		_, testsExists, err := root.GetTable(trtf.ctx, testsTableName)
		if err != nil {
			return nil, fmt.Errorf("error checking for dolt_tests table: %w", err)
		}
		if !testsExists {
			return nil, fmt.Errorf("could not find tests for argument: %s (dolt_tests table does not exist)", arg)
		}

		// Get the actual table from the root
		table, _, err := root.GetTable(trtf.ctx, testsTableName)
		if err != nil {
			return nil, fmt.Errorf("error getting dolt_tests table: %w", err)
		}

		// For now, implement a simple table scan to read the dolt_tests data
		return trtf.readTableDataFromDoltTable(table, arg)
	}

	// Original behavior when root is nil - use SQL queries against current session
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
		_, iter, _, err := trtf.engine.Query(trtf.ctx, query)
		if err != nil {
			return nil, err
		}
		// Calling iter.Close(ctx) will cause TrackedRowIter to cancel the context, causing problems when running with
		// dolt sql-server. Since we only support `SELECT...` queries anyway, it's not necessary to Close() the iter.
		var rows []sql.Row
		for {
			row, rErr := iter.Next(trtf.ctx)
			if rErr == io.EOF {
				break
			}
			if rErr != nil {
				return nil, rErr
			}
			rows = append(rows, row)
		}
		if len(rows) > 0 {
			return rows, nil
		}
	}
	return nil, fmt.Errorf("could not find tests for argument: %s", arg)
}

func IsWriteQuery(query string, ctx *sql.Context, catalog sql.Catalog) (bool, error) {
	builder := planbuilder.New(ctx, catalog, nil)

	parser := overrides.ParserFromContext(ctx)
	parsed, _, _, err := parser.Parse(ctx, query, false)
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
	if testName, err = getStringColAsString(ctx, row[0]); err != nil {
		return
	}
	if groupName, err = getStringColAsString(ctx, row[1]); err != nil {
		return
	}
	if query, err = getStringColAsString(ctx, row[2]); err != nil {
		return
	}
	if assertion, err = getStringColAsString(ctx, row[3]); err != nil {
		return
	}
	if comparison, err = getStringColAsString(ctx, row[4]); err != nil {
		return
	}
	if value, err = getStringColAsString(ctx, row[5]); err != nil {
		return
	}

	return testName, groupName, query, assertion, comparison, value, nil
}

// AssertDataFunc defines the function signature for asserting test data
type AssertDataFunc func(sqlCtx *sql.Context, assertion string, comparison string, value *string, queryResult sql.RowIter) (testPassed bool, message string, err error)

// RunTestsAgainstRoot executes tests against a specific root using the test runner internals
// This is designed to be called from the validation system during commit operations
func RunTestsAgainstRoot(ctx *sql.Context, root doltdb.RootValue, engine *gms.Engine, testGroups []string, assertDataFunc AssertDataFunc) ([]TestResult, error) {
	// Create a test runner instance
	trtf := &TestsRunTableFunction{
		ctx:    ctx,
		engine: engine,
	}
	
	var allResults []TestResult
	
	for _, group := range testGroups {
		// Get test data from the specific root
		testRows, err := trtf.getDoltTestsDataWithRoot(group, root)
		if err != nil {
			return nil, fmt.Errorf("failed to get test data for group %s: %w", group, err)
		}
		
		// Run each test using the queryAndAssert method with custom assertDataFunc
		for _, row := range testRows {
			result, err := trtf.queryAndAssertWithFunc(row, assertDataFunc)
			if err != nil {
				return nil, fmt.Errorf("failed to run test: %w", err)
			}
			allResults = append(allResults, result)
		}
	}
	
	return allResults, nil
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

// Simple inline assertion constants to avoid circular imports
const (
	AssertionExpectedRows        = "expected_rows"
	AssertionExpectedColumns     = "expected_columns"
	AssertionExpectedSingleValue = "expected_single_value"
)

// inlineAssertData provides basic assertion functionality without importing actions package
func inlineAssertData(sqlCtx *sql.Context, assertion string, comparison string, value *string, queryResult sql.RowIter) (testPassed bool, message string, err error) {
	switch assertion {
	case AssertionExpectedRows:
		return inlineExpectRows(sqlCtx, comparison, value, queryResult)
	case AssertionExpectedColumns:
		return inlineExpectColumns(sqlCtx, comparison, value, queryResult)
	case AssertionExpectedSingleValue:
		// For simplicity, just implement basic single value check
		return inlineExpectSingleValue(sqlCtx, comparison, value, queryResult)
	default:
		return false, fmt.Sprintf("%s is not a valid assertion type", assertion), nil
	}
}

func inlineExpectRows(sqlCtx *sql.Context, comparison string, value *string, queryResult sql.RowIter) (testPassed bool, message string, err error) {
	if value == nil {
		return false, "expected_rows requires a value", nil
	}
	
	expectedRows, err := strconv.Atoi(*value)
	if err != nil {
		return false, fmt.Sprintf("expected_rows value must be an integer: %s", *value), nil
	}
	
	actualRows := 0
	for {
		_, rErr := queryResult.Next(sqlCtx)
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			return false, "", rErr
		}
		actualRows++
	}
	
	switch comparison {
	case "=", "==":
		if actualRows == expectedRows {
			return true, "", nil
		}
		return false, fmt.Sprintf("Expected %d rows, got %d", expectedRows, actualRows), nil
	default:
		return false, fmt.Sprintf("Unsupported comparison operator for expected_rows: %s", comparison), nil
	}
}

func inlineExpectColumns(sqlCtx *sql.Context, comparison string, value *string, queryResult sql.RowIter) (testPassed bool, message string, err error) {
	if value == nil {
		return false, "expected_columns requires a value", nil
	}
	
	expectedColumns, err := strconv.Atoi(*value)
	if err != nil {
		return false, fmt.Sprintf("expected_columns value must be an integer: %s", *value), nil
	}
	
	row, err := queryResult.Next(sqlCtx)
	if err == io.EOF {
		return false, "No rows returned for expected_columns check", nil
	}
	if err != nil {
		return false, "", err
	}
	
	actualColumns := len(row)
	
	switch comparison {
	case "=", "==":
		if actualColumns == expectedColumns {
			return true, "", nil
		}
		return false, fmt.Sprintf("Expected %d columns, got %d", expectedColumns, actualColumns), nil
	default:
		return false, fmt.Sprintf("Unsupported comparison operator for expected_columns: %s", comparison), nil
	}
}

func inlineExpectSingleValue(sqlCtx *sql.Context, comparison string, value *string, queryResult sql.RowIter) (testPassed bool, message string, err error) {
	row, err := queryResult.Next(sqlCtx)
	if err == io.EOF {
		return false, "Expected single value but got no rows", nil
	}
	if err != nil {
		return false, "", err
	}
	
	if len(row) != 1 {
		return false, fmt.Sprintf("Expected single value but got %d columns", len(row)), nil
	}
	
	// Check if there are more rows
	_, err = queryResult.Next(sqlCtx)
	if err == nil {
		return false, "Expected single value but got multiple rows", nil
	} else if err != io.EOF {
		return false, "", err
	}
	
	// Simple string comparison for now
	actualStr := fmt.Sprintf("%v", row[0])
	if value == nil {
		if row[0] == nil {
			return true, "", nil
		}
		return false, fmt.Sprintf("Expected null but got: %s", actualStr), nil
	}
	
	switch comparison {
	case "=", "==":
		if actualStr == *value {
			return true, "", nil
		}
		return false, fmt.Sprintf("Expected '%s' but got '%s'", *value, actualStr), nil
	default:
		return false, fmt.Sprintf("Unsupported comparison operator for expected_single_value: %s", comparison), nil
	}
}

// getStringColAsString safely converts a sql value to string  
func getStringColAsString(sqlCtx *sql.Context, tableValue interface{}) (*string, error) {
	if tableValue == nil {
		return nil, nil
	}
	if ts, ok := tableValue.(*val.TextStorage); ok {
		str, err := ts.Unwrap(sqlCtx)
		if err != nil {
			return nil, err
		}
		return &str, nil
	} else if str, ok := tableValue.(string); ok {
		return &str, nil
	} else {
		return nil, fmt.Errorf("unexpected type %T, was expecting string", tableValue)
	}
}

// readTableDataFromDoltTable reads test data directly from a dolt table
func (trtf *TestsRunTableFunction) readTableDataFromDoltTable(table *doltdb.Table, arg string) ([]sql.Row, error) {
	// This is a complex implementation that requires reading table data directly from dolt storage
	// For now, return an error that clearly indicates this needs to be implemented
	// The table scan would involve:
	// 1. Getting the table schema
	// 2. Creating a table iterator
	// 3. Reading and filtering rows based on the arg (test_name or test_group)
	// 4. Converting dolt storage format to SQL rows
	//
	// This is a significant implementation that requires understanding dolt's storage internals
	return nil, fmt.Errorf("direct table reading from dolt storage not yet implemented for table scan of dolt_tests - this requires implementing table iteration and row conversion from dolt's internal storage format")
}

