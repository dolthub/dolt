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

package actions

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"
	"golang.org/x/exp/constraints"

	"github.com/dolthub/dolt/go/store/val"
)

const (
	AssertionExpectedRows        = "expected_rows"
	AssertionExpectedColumns     = "expected_columns"
	AssertionExpectedSingleValue = "expected_single_value"
)

// AssertData parses an assertion, comparison, and value, then returns the status of the test.
// Valid comparison are: "==", "!=", "<", ">", "<=", and ">=".
// testPassed indicates whether the test was successful or not.
// message is a string used to indicate test failures, and will not halt the overall process.
// message will be empty if the test passed.
// err indicates runtime failures and will stop dolt_test_run from proceeding.
func AssertData(sqlCtx *sql.Context, assertion string, comparison string, value string, queryResult *sql.RowIter) (testPassed bool, message string, err error) {
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

func expectSingleValue(sqlCtx *sql.Context, comparison string, value string, queryResult *sql.RowIter) (message string, err error) {
	row, err := (*queryResult).Next(sqlCtx)
	if err == io.EOF {
		return fmt.Sprintf("expected_single_value expects exactly one cell. Received 0 rows"), nil
	} else if err != nil {
		return "", err
	}

	if len(row) != 1 {
		return fmt.Sprintf("expected_single_value expects exactly one cell. Received multiple columns"), nil
	}
	_, err = (*queryResult).Next(sqlCtx)
	(*queryResult).Close(sqlCtx)
	if err == nil { //If multiple rows were given, we should error out
		return fmt.Sprintf("expected_single_value expects exactly one cell. Received multiple rows"), nil
	} else if err != io.EOF { // "True" error, so we should quit out
		return "", err
	}

	switch actualValue := row[0].(type) {
	case int32:
		expectedInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", value, actualValue), nil
		}
		return compareTestAssertion(comparison, int32(expectedInt), actualValue, AssertionExpectedSingleValue), nil
	case int64:
		expectedInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", value, actualValue), nil
		}
		return compareTestAssertion(comparison, expectedInt, actualValue, AssertionExpectedSingleValue), nil
	case uint32:
		expectedUint, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", value, actualValue), nil
		}
		return compareTestAssertion(comparison, uint32(expectedUint), actualValue, AssertionExpectedSingleValue), nil
	case float64:
		expectedFloat, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non float value '%s', with %f", value, actualValue), nil
		}
		return compareTestAssertion(comparison, expectedFloat, actualValue, AssertionExpectedSingleValue), nil
	case float32:
		expectedFloat, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return fmt.Sprintf("Could not compare non float value '%s', with %f", value, actualValue), nil
		}
		return compareTestAssertion(comparison, float32(expectedFloat), actualValue, AssertionExpectedSingleValue), nil
	case decimal.Decimal:
		expectedDecimal, err := decimal.NewFromString(value)
		if err != nil {
			return fmt.Sprintf("Could not compare non decimal value '%s', with %s", value, actualValue), nil
		}
		return compareDecimals(comparison, expectedDecimal, actualValue, AssertionExpectedSingleValue), nil
	case time.Time:
		expectedTime, format, err := parseTestsDate(value)
		if err != nil {
			return fmt.Sprintf("%s does not appear to be a valid date", value), nil
		}
		return compareDates(comparison, expectedTime, actualValue, format, AssertionExpectedSingleValue), nil
	case *val.TextStorage, string:
		actualString, err := GetStringColAsString(sqlCtx, actualValue)
		if err != nil {
			return "", err
		}
		return compareTestAssertion(comparison, value, actualString, AssertionExpectedSingleValue), nil
	default:
		if actualValue == nil {
			return compareNilValue(comparison, value, AssertionExpectedSingleValue), nil
		} else {
			return fmt.Sprintf("The type of %v is not supported. Open an issue at https://github.com/dolthub/dolt/issues to see it added", actualValue), nil
		}
	}
}

func expectRows(sqlCtx *sql.Context, comparison string, value string, queryResult *sql.RowIter) (message string, err error) {
	expectedRows, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Sprintf("cannot run assertion on non integer value: %s", value), nil
	}

	var numRows int
	for {
		_, err := (*queryResult).Next(sqlCtx)
		if err == io.EOF {
			break
		} else if err != nil {
			return "", err
		}
		numRows++
	}
	return compareTestAssertion(comparison, expectedRows, numRows, AssertionExpectedRows), nil
}

func expectColumns(sqlCtx *sql.Context, comparison string, value string, queryResult *sql.RowIter) (message string, err error) {
	expectedColumns, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Sprintf("cannot run assertion on non integer value: %s", value), nil
	}

	var numColumns int
	row, err := (*queryResult).Next(sqlCtx)
	if err != nil && err != io.EOF {
		(*queryResult).Close(sqlCtx)
		return "", err
	}
	numColumns = len(row)
	err = (*queryResult).Close(sqlCtx)
	if err != nil {
		return "", err
	}
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
		return fmt.Sprintf("%s is not a valid assertion type", comparison)
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
		return fmt.Sprintf("%s is not a valid assertion type", comparison)
	}
	return ""
}

// compareDecimals is a function used for comparing decimals.
// It takes in a comparison string from one of: "==", "!="
// It returns a string. The string is empty if the assertion passed, or has a message explaining the failure otherwise
func compareNilValue(comparison string, expectedValue, assertionType string) string {
	switch comparison {
	case "==":
		if strings.ToLower(expectedValue) != "null" {
			return fmt.Sprintf("Assertion failed: %s equal to %s, got NULL", assertionType, expectedValue)
		}
	case "!=":
		if strings.ToLower(expectedValue) == "null" {
			return fmt.Sprintf("Assertion failed: %s not equal to %s, got NULL", assertionType, strings.ToUpper(expectedValue))
		}
	}
	return ""
}

// GetStringColAsString is a function that returns a text column as a string.
// This is necessary as the dolt_tests system table returns *val.TextStorage types under certain situations,
// so we use a special parser to get the correct string values
func GetStringColAsString(sqlCtx *sql.Context, tableValue interface{}) (string, error) {
	if ts, ok := tableValue.(*val.TextStorage); ok {
		return ts.Unwrap(sqlCtx)
	} else if str, ok := tableValue.(string); ok {
		return str, nil
	} else if tableValue == nil {
		return "", nil
	} else {
		return "", fmt.Errorf("unexpected type %T, was expecting string", tableValue)
	}
}
