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

package dsess

import (
	"fmt"
	"io"
	"strings"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/store/val"
)

// runTestValidation executes test validation using the dolt_test_run() table function.
// It runs tests for the specified test groups during the given operation type.
func runTestValidation(ctx *sql.Context, testGroups []string, operationType string) error {
	// If no test groups specified, skip validation
	if len(testGroups) == 0 {
		return nil
	}

	// Get the DoltSession and provider directly (no reflection needed!)
	doltSession := ctx.Session.(*DoltSession)
	provider := doltSession.Provider()

	// Create an engine to execute queries
	engine := gms.NewDefault(provider)

	// Run tests for each group and collect failures
	var allFailures []string

	for _, group := range testGroups {
		var query string
		if group == "*" {
			// Run all tests
			query = "SELECT * FROM dolt_test_run()"
		} else {
			// Use proper MySQL parameter interpolation to prevent SQL injection
			var err error
			query, err = dbr.InterpolateForDialect("SELECT * FROM dolt_test_run(?)", []interface{}{group}, dialect.MySQL)
			if err != nil {
				return fmt.Errorf("failed to interpolate query for group %s: %w", group, err)
			}
		}

		// Execute the query using the engine
		_, iter, _, err := engine.Query(ctx, query)
		if err != nil {
			// If there are no dolt_tests to run for the specified group, that's an error
			return fmt.Errorf("failed to run tests for group %s: %w", group, err)
		}

		// Collect all rows from the iterator
		var rows []sql.Row
		for {
			row, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("error reading test results for group %s: %w", group, err)
			}
			rows = append(rows, row)
		}

		// If no rows returned, the group was not found
		if len(rows) == 0 {
			return fmt.Errorf("no tests found for group %s", group)
		}

		// Process results - any rows indicate test results (both pass and fail)
		failures, err := processTestResults(ctx, rows)
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
func processTestResults(ctx *sql.Context, rows []sql.Row) ([]string, error) {
	var failures []string

	for _, row := range rows {
		if len(row) < 5 {
			return nil, fmt.Errorf("unexpected row format from dolt_test_run()")
		}

		testName, err := getStringValue(ctx, row[0])
		if err != nil {
			return nil, fmt.Errorf("failed to read test_name: %w", err)
		}

		status, err := getStringValue(ctx, row[3])
		if err != nil {
			return nil, fmt.Errorf("failed to read status for test %s: %w", testName, err)
		}

		// If status is not "PASS", it's a failure (matches dolt_test_run.go:247)
		if status != "PASS" {
			message, err := getStringValue(ctx, row[4])
			if err != nil {
				message = "unknown error"
			}
			failures = append(failures, fmt.Sprintf("%s (%s)", testName, message))
		}
	}

	return failures, nil
}

// getStringValue safely converts a sql.Row value to string using the same pattern as CI code
func getStringValue(sqlCtx *sql.Context, tableValue interface{}) (string, error) {
	if ts, ok := tableValue.(*val.TextStorage); ok {
		return ts.Unwrap(sqlCtx)
	} else if str, ok := tableValue.(string); ok {
		return str, nil
	} else {
		return "", fmt.Errorf("unexpected type %T, was expecting string", tableValue)
	}
}