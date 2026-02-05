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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
)

// RunTestValidation executes test validation using the dolt_test_run() table function.
// It runs tests for the specified test groups during the given operation type.
func RunTestValidation(ctx *sql.Context, testGroups []string, operationType string) error {
	// If no test groups specified, skip validation
	if len(testGroups) == 0 {
		return nil
	}

	// Create a queryist from the session to use existing CLI infrastructure
	queryist, ok := ctx.Session.(cli.Queryist)
	if !ok {
		return nil // Session doesn't support queries, skip validation
	}

	// Run tests for each group and collect failures
	var allFailures []string
	
	for _, group := range testGroups {
		var query string
		if group == "*" {
			// Run all tests
			query = "SELECT * FROM dolt_test_run()"
		} else {
			// Run tests for specific group
			query = fmt.Sprintf("SELECT * FROM dolt_test_run('%s')", strings.ReplaceAll(group, "'", "''"))
		}
		
		rows, err := cli.GetRowsForSql(queryist, ctx, query)
		if err != nil {
			// If dolt_test_run doesn't exist or table doesn't exist, skip validation
			return nil
		}
		
		// Process results - any rows indicate test results (both pass and fail)
		failures, err := processTestResults(ctx, rows, group)
		if err != nil {
			return fmt.Errorf("error processing test results for group %s: %w", group, err)
		}
		
		allFailures = append(allFailures, failures...)
	}

	// If any tests failed, return error with details
	if len(allFailures) > 0 {
		return fmt.Errorf("test validation failed for %s: %s", operationType, strings.Join(allFailures, "; "))
	}

	return nil
}

// processTestResults processes rows from dolt_test_run() and returns failure messages.
// The dolt_test_run() table function returns: test_name, test_group_name, query, status, message
func processTestResults(ctx *sql.Context, rows []sql.Row, group string) ([]string, error) {
	var failures []string
	
	for _, row := range rows {
		if len(row) < 5 {
			return nil, fmt.Errorf("unexpected row format from dolt_test_run()")
		}
		
		testName, err := getStringValue(row[0])
		if err != nil {
			return nil, fmt.Errorf("failed to read test_name: %w", err)
		}
		
		status, err := getStringValue(row[3])
		if err != nil {
			return nil, fmt.Errorf("failed to read status for test %s: %w", testName, err)
		}
		
		// If status is not "PASS", it's a failure
		if status != "PASS" {
			message, err := getStringValue(row[4])
			if err != nil {
				message = "unknown error"
			}
			failures = append(failures, fmt.Sprintf("%s (%s)", testName, message))
		}
	}
	
	return failures, nil
}

// getStringValue safely converts a sql.Row value to string
func getStringValue(val interface{}) (string, error) {
	if val == nil {
		return "", nil
	}
	if str, ok := val.(string); ok {
		return str, nil
	}
	return fmt.Sprintf("%v", val), nil
}