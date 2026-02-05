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

package testvalidation

import (
	"fmt"
	"io"
	"strings"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// RunTestValidation executes test validation for the specified operation and test groups.
// It queries the dolt_tests table and runs tests that match the specified groups.
func RunTestValidation(ctx *sql.Context, testGroups []string, operationType string) error {
	// If no test groups specified, skip validation
	if len(testGroups) == 0 {
		return nil
	}

	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return nil // No database selected, skip validation
	}

	// Get the database provider from session
	provider, ok := ctx.Session.(sql.DatabaseProvider)
	if !ok {
		return nil // Session doesn't provide databases, skip validation
	}
	
	db, err := provider.Database(ctx, dbName)
	if err != nil {
		return nil // Database access error, skip validation
	}

	// Check if dolt_tests table exists
	tableNames, err := db.GetTableNames(ctx)
	if err != nil {
		return nil // Can't get table names, skip validation
	}

	hasTestsTable := false
	for _, tableName := range tableNames {
		if tableName == "dolt_tests" {
			hasTestsTable = true
			break
		}
	}

	if !hasTestsTable {
		return nil // No dolt_tests table, skip validation
	}

	// Create engine from provider
	engine := gms.NewDefault(provider)
	
	// Build query to get tests for the specified groups
	groupConditions := make([]string, len(testGroups))
	for i, group := range testGroups {
		if group == "*" {
			// Run all tests
			groupConditions = []string{"1 = 1"}
			break
		}
		groupConditions[i] = fmt.Sprintf("test_group = '%s'", group)
	}

	query := fmt.Sprintf("SELECT test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value FROM dolt_tests WHERE %s",
		strings.Join(groupConditions, " OR "))

	// Execute query to get tests
	_, iter, _, err := engine.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query dolt_tests table: %w", err)
	}

	var failedTests []string

	// Execute each test
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break // No more rows
		}
		if err != nil {
			return fmt.Errorf("failed to read test row: %w", err)
		}
		if row == nil {
			break // No more rows
		}

		testName := row[0].(string)
		testGroup := row[1].(string)
		testQuery := row[2].(string)
		assertionType := row[3].(string)
		assertionComparator := row[4].(string)
		assertionValue := row[5]

		// Execute the test query
		_, testIter, _, err := engine.Query(ctx, testQuery)
		if err != nil {
			failedTests = append(failedTests, fmt.Sprintf("%s (query failed: %v)", testName, err))
			continue
		}

		// Get the first result row
		resultRow, err := testIter.Next(ctx)
		if err == io.EOF || resultRow == nil {
			failedTests = append(failedTests, fmt.Sprintf("%s (no result returned)", testName))
			continue
		}
		if err != nil {
			failedTests = append(failedTests, fmt.Sprintf("%s (result read failed: %v)", testName, err))
			continue
		}

		// Validate the result based on assertion type
		passed, err := validateTestAssertion(ctx, resultRow[0], assertionType, assertionComparator, assertionValue)
		if err != nil {
			failedTests = append(failedTests, fmt.Sprintf("%s (assertion validation failed: %v)", testName, err))
			continue
		}

		if !passed {
			failedTests = append(failedTests, fmt.Sprintf("%s (expected %s %s %v, got %v)", 
				testName, assertionType, assertionComparator, assertionValue, resultRow[0]))
		}

		_ = testGroup // Used for potential filtering/reporting
	}

	// If any tests failed, return error
	if len(failedTests) > 0 {
		return fmt.Errorf("test validation failed for %s: %s", operationType, strings.Join(failedTests, "; "))
	}

	return nil
}

// validateTestAssertion validates a test result against the expected assertion
func validateTestAssertion(ctx *sql.Context, actual interface{}, assertionType, comparator string, expected interface{}) (bool, error) {
	switch assertionType {
	case "expected_single_value":
		return validateSingleValue(ctx, actual, comparator, expected)
	default:
		return false, fmt.Errorf("unsupported assertion type: %s", assertionType)
	}
}

// validateSingleValue validates a single value assertion
func validateSingleValue(ctx *sql.Context, actual interface{}, comparator string, expected interface{}) (bool, error) {
	switch comparator {
	case "==":
		actualStr := fmt.Sprintf("%v", actual)
		expectedStr := fmt.Sprintf("%v", expected)
		return actualStr == expectedStr, nil
	case "!=":
		actualStr := fmt.Sprintf("%v", actual)
		expectedStr := fmt.Sprintf("%v", expected)
		return actualStr != expectedStr, nil
	case ">":
		actualNum, _, err := types.Float64.Convert(ctx, actual)
		if err != nil {
			return false, fmt.Errorf("cannot convert actual value to number: %v", err)
		}
		expectedNum, _, err := types.Float64.Convert(ctx, expected)
		if err != nil {
			return false, fmt.Errorf("cannot convert expected value to number: %v", err)
		}
		return actualNum.(float64) > expectedNum.(float64), nil
	case "<":
		actualNum, _, err := types.Float64.Convert(ctx, actual)
		if err != nil {
			return false, fmt.Errorf("cannot convert actual value to number: %v", err)
		}
		expectedNum, _, err := types.Float64.Convert(ctx, expected)
		if err != nil {
			return false, fmt.Errorf("cannot convert expected value to number: %v", err)
		}
		return actualNum.(float64) < expectedNum.(float64), nil
	case ">=":
		actualNum, _, err := types.Float64.Convert(ctx, actual)
		if err != nil {
			return false, fmt.Errorf("cannot convert actual value to number: %v", err)
		}
		expectedNum, _, err := types.Float64.Convert(ctx, expected)
		if err != nil {
			return false, fmt.Errorf("cannot convert expected value to number: %v", err)
		}
		return actualNum.(float64) >= expectedNum.(float64), nil
	case "<=":
		actualNum, _, err := types.Float64.Convert(ctx, actual)
		if err != nil {
			return false, fmt.Errorf("cannot convert actual value to number: %v", err)
		}
		expectedNum, _, err := types.Float64.Convert(ctx, expected)
		if err != nil {
			return false, fmt.Errorf("cannot convert expected value to number: %v", err)
		}
		return actualNum.(float64) <= expectedNum.(float64), nil
	default:
		return false, fmt.Errorf("unsupported comparator: %s", comparator)
	}
}