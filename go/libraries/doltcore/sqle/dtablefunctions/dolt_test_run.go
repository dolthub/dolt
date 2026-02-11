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
	"time"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"
	"github.com/shopspring/decimal"
	"golang.org/x/exp/constraints"

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
			testPassed, message, err = AssertData(trtf.ctx, *assertion, *comparison, value, queryResult)
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

// AssertData parses an assertion, comparison, and value, then returns the status of the test.
// Valid comparison are: "==", "!=", "<", ">", "<=", and ">=".
// testPassed indicates whether the test was successful or not.
// message is a string used to indicate test failures, and will not halt the overall process.
// message will be empty if the test passed.
// err indicates runtime failures and will stop dolt_test_run from proceeding.
func AssertData(sqlCtx *sql.Context, assertion string, comparison string, value *string, queryResult sql.RowIter) (testPassed bool, message string, err error) {
	switch assertion {
	case AssertionExpectedRows:
		message, err = expectRows(sqlCtx, comparison, value, queryResult)
	case AssertionExpectedColumns:
		message, err = expectColumns(sqlCtx, comparison, value, queryResult)
	case AssertionExpectedSingleValue:
		message, err = expectSingleValue(sqlCtx, comparison, value, queryResult)
	default:
		return false, fmt.Sprintf("%s is not a valid assertion type", assertion), nil
	}

	if err != nil {
		return false, "", err
	} else if message != "" {
		return false, message, nil
	}
	return true, "", nil
}

func expectSingleValue(sqlCtx *sql.Context, comparison string, value *string, queryResult sql.RowIter) (message string, err error) {
	row, err := queryResult.Next(sqlCtx)
	if err == io.EOF {
		return fmt.Sprintf("expected_single_value expects exactly one cell. Received 0 rows"), nil
	} else if err != nil {
		return "", err
	}

	if len(row) != 1 {
		return fmt.Sprintf("expected_single_value expects exactly one cell. Received multiple columns"), nil
	}
	_, err = queryResult.Next(sqlCtx)
	if err == nil { //If multiple rows were given, we should error out
		return fmt.Sprintf("expected_single_value expects exactly one cell. Received multiple rows"), nil
	} else if err != io.EOF { // "True" error, so we should quit out
		return "", err
	}

	if value == nil { // If we're expecting a null value, we don't need to type switch
		return compareNullValue(comparison, row[0], AssertionExpectedSingleValue), nil
	}

	// Check if the expected value is a boolean string, and if so, coerce the actual value to boolean, with the exception
	// of "0" and "1", which are valid integers and are covered below.
	if *value != "0" && *value != "1" {
		if expectedBool, err := strconv.ParseBool(*value); err == nil {
			actualBool, boolErr := getInterfaceAsBool(row[0])
			if boolErr != nil {
				return fmt.Sprintf("Could not convert value to boolean: %v", boolErr), nil
			}
			return compareBooleans(comparison, expectedBool, actualBool, AssertionExpectedSingleValue), nil
		}
	}

	switch actualValue := row[0].(type) {
	case int8:
		expectedInt, err := strconv.ParseInt(*value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, int8(expectedInt), actualValue, AssertionExpectedSingleValue), nil
	case int16:
		expectedInt, err := strconv.ParseInt(*value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, int16(expectedInt), actualValue, AssertionExpectedSingleValue), nil
	case int32:
		expectedInt, err := strconv.ParseInt(*value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, int32(expectedInt), actualValue, AssertionExpectedSingleValue), nil
	case int64:
		expectedInt, err := strconv.ParseInt(*value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, expectedInt, actualValue, AssertionExpectedSingleValue), nil
	case int:
		expectedInt, err := strconv.ParseInt(*value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, int(expectedInt), actualValue, AssertionExpectedSingleValue), nil
	case uint8:
		expectedUint, err := strconv.ParseUint(*value, 10, 32)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, uint8(expectedUint), actualValue, AssertionExpectedSingleValue), nil
	case uint16:
		expectedUint, err := strconv.ParseUint(*value, 10, 32)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, uint16(expectedUint), actualValue, AssertionExpectedSingleValue), nil
	case uint32:
		expectedUint, err := strconv.ParseUint(*value, 10, 32)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, uint32(expectedUint), actualValue, AssertionExpectedSingleValue), nil
	case uint64:
		expectedUint, err := strconv.ParseUint(*value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, expectedUint, actualValue, AssertionExpectedSingleValue), nil
	case uint:
		expectedUint, err := strconv.ParseUint(*value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, uint(expectedUint), actualValue, AssertionExpectedSingleValue), nil
	case float64:
		expectedFloat, err := strconv.ParseFloat(*value, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non float value '%s', with %f", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, expectedFloat, actualValue, AssertionExpectedSingleValue), nil
	case float32:
		expectedFloat, err := strconv.ParseFloat(*value, 32)
		if err != nil {
			return fmt.Sprintf("Could not compare non float value '%s', with %f", *value, actualValue), nil
		}
		return compareTestAssertion(comparison, float32(expectedFloat), actualValue, AssertionExpectedSingleValue), nil
	case decimal.Decimal:
		expectedDecimal, err := decimal.NewFromString(*value)
		if err != nil {
			return fmt.Sprintf("Could not compare non decimal value '%s', with %s", *value, actualValue), nil
		}
		return compareDecimals(comparison, expectedDecimal, actualValue, AssertionExpectedSingleValue), nil
	case time.Time:
		expectedTime, format, err := parseTestsDate(*value)
		if err != nil {
			return fmt.Sprintf("%s does not appear to be a valid date", *value), nil
		}
		return compareDates(comparison, expectedTime, actualValue, format, AssertionExpectedSingleValue), nil
	case *val.TextStorage, string:
		actualString, err := GetStringColAsString(sqlCtx, actualValue)
		if err != nil {
			return "", err
		}
		return compareTestAssertion(comparison, *value, *actualString, AssertionExpectedSingleValue), nil
	default:
		return fmt.Sprintf("Type %T is not supported. Open an issue at https://github.com/dolthub/dolt/issues to see it added", actualValue), nil
	}
}

func expectRows(sqlCtx *sql.Context, comparison string, value *string, queryResult sql.RowIter) (message string, err error) {
	if value == nil {
		return "null is not a valid assertion for expected_rows", nil
	}
	expectedRows, err := strconv.Atoi(*value)
	if err != nil {
		return fmt.Sprintf("cannot run assertion on non integer value: %s", *value), nil
	}

	var numRows int
	for {
		_, err := queryResult.Next(sqlCtx)
		if err == io.EOF {
			break
		} else if err != nil {
			return "", err
		}
		numRows++
	}
	return compareTestAssertion(comparison, expectedRows, numRows, AssertionExpectedRows), nil
}

func expectColumns(sqlCtx *sql.Context, comparison string, value *string, queryResult sql.RowIter) (message string, err error) {
	if value == nil {
		return "null is not a valid assertion for expected_rows", nil
	}
	expectedColumns, err := strconv.Atoi(*value)
	if err != nil {
		return fmt.Sprintf("cannot run assertion on non integer value: %s", *value), nil
	}

	var numColumns int
	row, err := queryResult.Next(sqlCtx)
	if err != nil && err != io.EOF {
		return "", err
	}
	numColumns = len(row)
	return compareTestAssertion(comparison, expectedColumns, numColumns, AssertionExpectedColumns), nil
}

// compareTestAssertion is a generic function used for comparing string, ints, floats.
// It takes in a comparison string from one of: "==", "!=", "<", ">", "<=", ">="
// It returns a string. The string is empty if the assertion passed, or has a message explaining the failure otherwise
func compareTestAssertion[T constraints.Ordered](comparison string, expectedValue, actualValue T, assertionType string) string {
	switch comparison {
	case "==":
		if actualValue != expectedValue {
			return fmt.Sprintf("Assertion failed: %s equal to %v, got %v", assertionType, expectedValue, actualValue)
		}
	case "!=":
		if actualValue == expectedValue {
			return fmt.Sprintf("Assertion failed: %s not equal to %v, got %v", assertionType, expectedValue, actualValue)
		}
	case "<":
		if actualValue >= expectedValue {
			return fmt.Sprintf("Assertion failed: %s less than %v, got %v", assertionType, expectedValue, actualValue)
		}
	case "<=":
		if actualValue > expectedValue {
			return fmt.Sprintf("Assertion failed: %s less than or equal to %v, got %v", assertionType, expectedValue, actualValue)
		}
	case ">":
		if actualValue <= expectedValue {
			return fmt.Sprintf("Assertion failed: %s greater than %v, got %v", assertionType, expectedValue, actualValue)
		}
	case ">=":
		if actualValue < expectedValue {
			return fmt.Sprintf("Assertion failed: %s greater than or equal to %v, got %v", assertionType, expectedValue, actualValue)
		}
	default:
		return fmt.Sprintf("%s is not a valid comparison type", comparison)
	}
	return ""
}

// parseTestsDate is an internal function that parses the queried string according to allowed time formats for dolt_tests.
// It returns the parsed time, the format that succeeded, and an error if applicable.
func parseTestsDate(value string) (parsedTime time.Time, format string, err error) {
	// List of valid formats
	formats := []string{
		time.DateOnly,
		time.DateTime,
		time.TimeOnly,
		time.RFC3339,
		time.RFC1123Z,
	}

	for _, format := range formats {
		if parsedTime, parseErr := time.Parse(format, value); parseErr == nil {
			return parsedTime, format, nil
		} else {
			err = parseErr
		}
	}
	return time.Time{}, "", err
}

// compareDates is a function used for comparing time values.
// It takes in a comparison string from one of: "==", "!=", "<", ">", "<=", ">="
// It returns a string. The string is empty if the assertion passed, or has a message explaining the failure otherwise
func compareDates(comparison string, expectedValue, realValue time.Time, format string, assertionType string) string {
	expectedStr := expectedValue.Format(format)
	realStr := realValue.Format(format)
	switch comparison {
	case "==":
		if !expectedValue.Equal(realValue) {
			return fmt.Sprintf("Assertion failed: %s equal to %s, got %s", assertionType, expectedStr, realStr)
		}
	case "!=":
		if expectedValue.Equal(realValue) {
			return fmt.Sprintf("Assertion failed: %s not equal to %s, got %s", assertionType, expectedStr, realStr)
		}
	case "<":
		if realValue.Equal(expectedValue) || realValue.After(expectedValue) {
			return fmt.Sprintf("Assertion failed: %s less than %s, got %s", assertionType, expectedStr, realStr)
		}
	case "<=":
		if realValue.After(expectedValue) {
			return fmt.Sprintf("Assertion failed: %s less than or equal to %s, got %s", assertionType, expectedStr, realStr)
		}
	case ">":
		if realValue.Before(expectedValue) || realValue.Equal(expectedValue) {
			return fmt.Sprintf("Assertion failed: %s greater than %s, got %s", assertionType, expectedStr, realStr)
		}
	case ">=":
		if realValue.Before(expectedValue) {
			return fmt.Sprintf("Assertion failed: %s greater than or equal to %s, got %s", assertionType, expectedStr, realStr)
		}
	default:
		return fmt.Sprintf("%s is not a valid comparison type", comparison)
	}
	return ""
}

// compareDecimals is a function used for comparing decimals.
// It takes in a comparison string from one of: "==", "!=", "<", ">", "<=", ">="
// It returns a string. The string is empty if the assertion passed, or has a message explaining the failure otherwise
func compareDecimals(comparison string, expectedValue, realValue decimal.Decimal, assertionType string) string {
	switch comparison {
	case "==":
		if !expectedValue.Equal(realValue) {
			return fmt.Sprintf("Assertion failed: %s equal to %v, got %v", assertionType, expectedValue, realValue)
		}
	case "!=":
		if expectedValue.Equal(realValue) {
			return fmt.Sprintf("Assertion failed: %s not equal to %v, got %v", assertionType, expectedValue, realValue)
		}
	case "<":
		if realValue.GreaterThanOrEqual(expectedValue) {
			return fmt.Sprintf("Assertion failed: %s less than %v, got %v", assertionType, expectedValue, realValue)
		}
	case "<=":
		if realValue.GreaterThan(expectedValue) {
			return fmt.Sprintf("Assertion failed: %s less than or equal to %v, got %v", assertionType, expectedValue, realValue)
		}
	case ">":
		if realValue.LessThanOrEqual(expectedValue) {
			return fmt.Sprintf("Assertion failed: %s greater than %v, got %v", assertionType, expectedValue, realValue)
		}
	case ">=":
		if realValue.LessThan(expectedValue) {
			return fmt.Sprintf("Assertion failed: %s greater than or equal to %v, got %v", assertionType, expectedValue, realValue)
		}
	default:
		return fmt.Sprintf("%s is not a valid comparison type", comparison)
	}
	return ""
}

// getTinyIntColAsBool returns the value interface{} as a bool
// This is necessary because the query engine may return a tinyint column as a bool, int, or other types.
// Based on GetTinyIntColAsBool from commands/utils.go, which we can't depend on here due to package cycles.
func getInterfaceAsBool(col interface{}) (bool, error) {
	switch v := col.(type) {
	case bool:
		return v, nil
	case int:
		return v == 1, nil
	case int8:
		return v == 1, nil
	case int16:
		return v == 1, nil
	case int32:
		return v == 1, nil
	case int64:
		return v == 1, nil
	case uint:
		return v == 1, nil
	case uint8:
		return v == 1, nil
	case uint16:
		return v == 1, nil
	case uint32:
		return v == 1, nil
	case uint64:
		return v == 1, nil
	case string:
		return v == "1", nil
	default:
		return false, fmt.Errorf("unexpected type %T, was expecting bool, int, or string", v)
	}
}

// compareBooleans is a function used for comparing boolean values.
// It takes in a comparison string from one of: "==", "!="
// It returns a string. The string is empty if the assertion passed, or has a message explaining the failure otherwise
func compareBooleans(comparison string, expectedValue, realValue bool, assertionType string) string {
	switch comparison {
	case "==":
		if expectedValue != realValue {
			return fmt.Sprintf("Assertion failed: %s equal to %t, got %t", assertionType, expectedValue, realValue)
		}
	case "!=":
		if expectedValue == realValue {
			return fmt.Sprintf("Assertion failed: %s not equal to %t, got %t", assertionType, expectedValue, realValue)
		}
	default:
		return fmt.Sprintf("%s is not a valid comparison for boolean values. Only '==' and '!=' are supported", comparison)
	}
	return ""
}

// compareNullValue is a function used for comparing a null value.
// It takes in a comparison string from one of: "==", "!="
// It returns a string. The string is empty if the assertion passed, or has a message explaining the failure otherwise
func compareNullValue(comparison string, actualValue interface{}, assertionType string) string {
	switch comparison {
	case "==":
		if actualValue != nil {
			return fmt.Sprintf("Assertion failed: %s equal to NULL, got %v", assertionType, actualValue)
		}
	case "!=":
		if actualValue == nil {
			return fmt.Sprintf("Assertion failed: %s not equal to NULL, got NULL", assertionType)
		}
	default:
		return fmt.Sprintf("%s is not a valid comparison for NULL values", comparison)
	}
	return ""
}

// GetStringColAsString is a function that returns a text column as a string.
// This is necessary as the dolt_tests system table returns *val.TextStorage types under certain situations,
// so we use a special parser to get the correct string values
func GetStringColAsString(sqlCtx *sql.Context, tableValue interface{}) (*string, error) {
	if ts, ok := tableValue.(*val.TextStorage); ok {
		str, err := ts.Unwrap(sqlCtx)
		return &str, err
	} else if str, ok := tableValue.(string); ok {
		return &str, nil
	} else if tableValue == nil {
		return nil, nil
	} else {
		return nil, fmt.Errorf("unexpected type %T, was expecting string", tableValue)
	}
}
