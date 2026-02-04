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

package sqle

import (
	"fmt"
	"io"
	"strings"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// GetCommitRunTestGroups returns the test groups to run for commit operations
// Returns empty slice if no tests should be run, ["*"] if all tests should be run,
// or specific group names if only those groups should be run
func GetCommitRunTestGroups() []string {
	_, val, ok := sql.SystemVariables.GetGlobal(dsess.DoltCommitRunTestGroups)
	if !ok {
		return nil
	}
	if stringVal, ok := val.(string); ok && stringVal != "" {
		if stringVal == "*" {
			return []string{"*"}
		}
		// Split by comma and trim whitespace
		groups := strings.Split(stringVal, ",")
		for i, group := range groups {
			groups[i] = strings.TrimSpace(group)
		}
		return groups
	}
	return nil
}

// GetPushRunTestGroups returns the test groups to run for push operations
// Returns empty slice if no tests should be run, ["*"] if all tests should be run,
// or specific group names if only those groups should be run
func GetPushRunTestGroups() []string {
	_, val, ok := sql.SystemVariables.GetGlobal(dsess.DoltPushRunTestGroups)
	if !ok {
		return nil
	}
	if stringVal, ok := val.(string); ok && stringVal != "" {
		if stringVal == "*" {
			return []string{"*"}
		}
		// Split by comma and trim whitespace
		groups := strings.Split(stringVal, ",")
		for i, group := range groups {
			groups[i] = strings.TrimSpace(group)
		}
		return groups
	}
	return nil
}

// RunTestValidation executes dolt_tests validation based on the specified test groups
// If testGroups is empty, no validation is performed
// If testGroups contains "*", all tests are run
// Otherwise, only tests in the specified groups are run
// Returns error if tests fail and should abort the operation
func RunTestValidation(ctx *sql.Context, engine *gms.Engine, testGroups []string, operationType string, logger io.Writer) error {
	// If no test groups specified, skip validation
	if len(testGroups) == 0 {
		return nil
	}

	// Check if dolt_tests table exists
	db := ctx.GetCurrentDatabase()
	if db == "" {
		return nil // No database selected, can't run tests
	}

	database, err := engine.Analyzer.Catalog.Database(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get database: %w", err)
	}

	tables, err := database.GetTableNames(ctx)
	if err != nil {
		return fmt.Errorf("failed to get table names: %w", err)
	}

	hasTestsTable := false
	for _, table := range tables {
		if table == "dolt_tests" {
			hasTestsTable = true
			break
		}
	}

	// If no dolt_tests table, nothing to validate
	if !hasTestsTable {
		return nil
	}

	// Build query to run tests
	var query string
	if len(testGroups) == 1 && testGroups[0] == "*" {
		// Run all tests
		query = "SELECT * FROM dolt_test_run()"
	} else {
		// Run specific test groups
		groupArgs := make([]string, len(testGroups))
		for i, group := range testGroups {
			groupArgs[i] = fmt.Sprintf("'%s'", group)
		}
		query = fmt.Sprintf("SELECT * FROM dolt_test_run(%s)", strings.Join(groupArgs, ", "))
	}

	// Execute test query
	_, iter, _, err := engine.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute dolt_test_run: %w", err)
	}
	defer iter.Close(ctx)

	// Process test results
	var failures []TestFailure
	totalTests := 0

	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read test results: %w", err)
		}

		totalTests++

		// Parse test result row: test_name, test_group_name, query, status, message
		testName := ""
		if row[0] != nil {
			testName = row[0].(string)
		}

		testGroup := ""
		if row[1] != nil {
			testGroup = row[1].(string)
		}

		testQuery := ""
		if row[2] != nil {
			testQuery = row[2].(string)
		}

		status := ""
		if row[3] != nil {
			status = row[3].(string)
		}

		message := ""
		if row[4] != nil {
			message = row[4].(string)
		}

		// Check if test failed
		if status != "PASS" {
			failures = append(failures, TestFailure{
				TestName:     testName,
				TestGroup:    testGroup,
				Query:        testQuery,
				ErrorMessage: message,
			})
		}
	}

	// Log results
	if logger != nil {
		if len(failures) == 0 {
			fmt.Fprintf(logger, "✓ All %d tests passed\n", totalTests)
		} else {
			fmt.Fprintf(logger, "✗ %d of %d tests failed\n", len(failures), totalTests)
		}
	}

	// Handle failures - always abort on failure for now
	if len(failures) > 0 {
		return fmt.Errorf("%s aborted: %d test(s) failed\n%s", operationType, len(failures), formatTestFailures(failures))
	}

	return nil
}

// TestFailure represents a single failed test
type TestFailure struct {
	TestName     string
	TestGroup    string
	Query        string
	Expected     string
	Actual       string
	ErrorMessage string
}

// formatTestFailures creates a human-readable summary of test failures
func formatTestFailures(failures []TestFailure) string {
	var sb strings.Builder
	for i, failure := range failures {
		if i > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(fmt.Sprintf("  • %s", failure.TestName))
		if failure.TestGroup != "" {
			sb.WriteString(fmt.Sprintf(" (group: %s)", failure.TestGroup))
		}
		if failure.ErrorMessage != "" {
			sb.WriteString(fmt.Sprintf(": %s", failure.ErrorMessage))
		}
	}
	return sb.String()
}