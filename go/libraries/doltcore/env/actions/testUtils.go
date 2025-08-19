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
	"io"
	"strconv"
	"strings"
)

func AssertData(sqlCtx *sql.Context, assertion string, queryResult *sql.RowIter) (result string, err error) {
	parts := strings.Split(strings.TrimSpace(assertion), " ") //TODO FLESH THIS OUT A BIT MORE PROBABLY
	if len(parts) != 3 {
		return "Unexpected assertion format", nil
	}

	expectedValue, err := strconv.Atoi(parts[2])
	if err != nil {
		return "", err
	}
	switch parts[0] {
	case "expected_rows":
		result, err = expectRows(sqlCtx, parts[1], expectedValue, queryResult)
	case "expected_columns":
		result, err = expectColumns(sqlCtx, parts[1], expectedValue, queryResult)
	default:
		return fmt.Sprintf("'%s' is not a valid assertion type", parts[0]), nil
	}

	return result, err
}

func expectRows(sqlCtx *sql.Context, comparison string, expectedRows int, queryResult *sql.RowIter) (result string, err error) {
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

func expectColumns(sqlCtx *sql.Context, comparison string, expectedColumns int, queryResult *sql.RowIter) (result string, err error) {
	var numColumns int
	row, err := (*queryResult).Next(sqlCtx)
	if err != nil && err != io.EOF {
		return "", err
	} else if err == nil {
		numColumns = len(row)
	}

	return compareTestAssertion(comparison, expectedColumns, numColumns, "column count"), nil
}

func compareTestAssertion(comparison string, expectedValue int, realValue int, assertionType string) string {
	switch comparison {
	case "==":
		if realValue != expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s equal to %d, got %d", assertionType, expectedValue, realValue)
		}
	case "!=":
		if realValue == expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s not equal to %d, got %d", assertionType, expectedValue, realValue)
		}
	case "<":
		if realValue >= expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s less than %d, got %d", assertionType, expectedValue, realValue)
		}
	case "<=":
		if realValue > expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s less than or equal to %d, got %d", assertionType, expectedValue, realValue)
		}
	case ">":
		if realValue <= expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s greater than %d, got %d", assertionType, expectedValue, realValue)
		}
	case ">=":
		if realValue < expectedValue {
			return fmt.Sprintf("Assertion failed: expected %s greater than or equal to %d, got %d", assertionType, expectedValue, realValue)
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
	} else {
		return "", fmt.Errorf("unexpected type %T, was expecting string", tableValue)
	}
}
