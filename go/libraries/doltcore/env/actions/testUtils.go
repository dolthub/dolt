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
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/exp/constraints"
	"io"
	"reflect"
	"strconv"
	"time"
)

func AssertData(sqlCtx *sql.Context, assertion string, comparison string, value string, queryResult *sql.RowIter) (result string, err error) {
	switch assertion {
	case "expected_rows":
		result, err = expectRows(sqlCtx, comparison, value, queryResult)
	case "expected_columns":
		result, err = expectColumns(sqlCtx, comparison, value, queryResult)
	case "expected_single_value":
		result, err = expectSingleValue(sqlCtx, comparison, value, queryResult)
	default:
		return fmt.Sprintf("%s is not a valid assertion type", assertion), nil
	}

	return result, err
}

func expectSingleValue(sqlCtx *sql.Context, comparison string, value string, queryResult *sql.RowIter) (result string, err error) {
	row, err := (*queryResult).Next(sqlCtx)
	if err == io.EOF {
		return fmt.Sprintf("expected_single_value expects exactly one cell"), nil
	} else if err != nil {
		return "", err
	}

	_, err = (*queryResult).Next(sqlCtx)
	if err == nil { //If multiple cells were given, we should error out
		return fmt.Sprintf("expected_single_value expects exactly one cell"), nil
	} else if err != io.EOF { // "True" error, so we should quit out
		return "", err
	}

	if len(row) != 1 {
		return fmt.Sprintf("expected_single_value expects exactly one cell"), nil
	}

	switch maybeActual := row[0].(type) {
	case int, int8, int16, int32, int64:
		expectedInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", value, maybeActual), nil
		}
		actualInt := reflect.ValueOf(maybeActual).Int()
		return compareTestAssertion(comparison, expectedInt, actualInt, "single value"), nil
	case uint, uint8, uint16, uint32, uint64:
		expectedUint, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non integer value '%s', with %d", value, maybeActual), nil
		}
		actualUint := row[0].(uint64)
		return compareTestAssertion(comparison, expectedUint, actualUint, "single value"), nil
	case float32, float64:
		expectedFloat, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Sprintf("Could not compare non float value '%s', with %f", value, maybeActual), nil
		}
		actualFloat := row[0].(float64)
		return compareTestAssertion(comparison, expectedFloat, actualFloat, "single value"), nil
	case time.Time:
		expectedTime, err := time.Parse(time.DateOnly, value)
		if err != nil {
			return fmt.Sprintf("%s does not appear to be a valid date", value), nil
		}
		return compareDates(comparison, expectedTime, maybeActual, "single value"), nil
	case *val.TextStorage, string:
		actualString, err := GetStringColAsString(sqlCtx, maybeActual)
		if err != nil {
			return "", err
		}
		return compareTestAssertion(comparison, value, actualString, "single value"), nil
	default:
		return fmt.Sprintf("The type of %s is not supported. Open an issue at https://github.com/dolthub/dolt/issues to see it added", maybeActual), nil
	}
}

func expectRows(sqlCtx *sql.Context, comparison string, value string, queryResult *sql.RowIter) (result string, err error) {
	expectedRows, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Sprintf("Received non integer value: %s", value), nil
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
	return compareTestAssertion(comparison, expectedRows, numRows, "row count"), nil
}

func expectColumns(sqlCtx *sql.Context, comparison string, value string, queryResult *sql.RowIter) (result string, err error) {
	expectedColumns, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Sprintf("Received non integer value: %s", value), nil
	}

	var numColumns int
	row, err := (*queryResult).Next(sqlCtx)
	if err != nil && err != io.EOF {
		return "", err
	} else if err == nil {
		numColumns = len(row)
	}

	return compareTestAssertion(comparison, expectedColumns, numColumns, "column count"), nil
}

func compareTestAssertion[T constraints.Ordered](comparison string, expectedValue, realValue T, assertionType string) string {
	switch comparison {
	case "==":
		if realValue != expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s equal to %v, got %v", assertionType, expectedValue, realValue)
		}
	case "!=":
		if realValue == expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s not equal to %v, got %v", assertionType, expectedValue, realValue)
		}
	case "<":
		if realValue >= expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s less than %v, got %v", assertionType, expectedValue, realValue)
		}
	case "<=":
		if realValue > expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s less than or equal to %v, got %v", assertionType, expectedValue, realValue)
		}
	case ">":
		if realValue <= expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s greater than %v, got %v", assertionType, expectedValue, realValue)
		}
	case ">=":
		if realValue < expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s greater than or equal to %v, got %v", assertionType, expectedValue, realValue)
		}
	default:
		return fmt.Sprintf("%s is not a valid assertion type", comparison)
	}
	return ""
}

func compareDates(comparison string, expectedValue, realValue time.Time, assertionType string) string {
	switch comparison {
	case "==":
		if !expectedValue.Equal(realValue) {
			return fmt.Sprintf("Assertion failed: expected %s equal to %s, got %s", assertionType, expectedValue, realValue)
		}
	case "!=":
		if expectedValue.Equal(realValue) {
			return fmt.Sprintf("Assertion failed: expected %s not equal to %s, got %s", assertionType, expectedValue, realValue)
		}
	case "<":
		if realValue.Equal(expectedValue) || realValue.After(expectedValue) {
			return fmt.Sprintf("Assertion failed: expected %s less than %s, got %s", assertionType, expectedValue, realValue)
		}
	case "<=":
		if realValue.After(expectedValue) {
			return fmt.Sprintf("Assertion failed: expected %s less than or equal to %s, got %s", assertionType, expectedValue, realValue)
		}
	case ">":
		if realValue.Before(expectedValue) || realValue.Equal(expectedValue) {
			return fmt.Sprintf("Assertion failed: expected %s greater than %s, got %s", assertionType, expectedValue.Format(time.DateOnly), realValue.Format(time.DateOnly))
		}
	case ">=":
		if realValue.Before(expectedValue) {
			return fmt.Sprintf("Assertion failed: expected %s greater than or equal to %s, got %s", assertionType, expectedValue, realValue)
		}
	default:
		return fmt.Sprintf("%s is not a valid assertion type", comparison)
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
	} else {
		return "", fmt.Errorf("unexpected type %T, was expecting string", tableValue)
	}
}
