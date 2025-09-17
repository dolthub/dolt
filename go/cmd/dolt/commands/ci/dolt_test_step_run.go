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

package ci

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"gopkg.in/yaml.v3"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/dolt_ci"
)

// runDoltTestStep evaluates a Dolt Test step per selection rules and requires all selected tests to PASS.
// It returns a human-readable summary of individual test results and an error aggregating any failures.
func runDoltTestStep(sqlCtx *sql.Context, queryist cli.Queryist, dt *dolt_ci.DoltTestStep) (string, error) {
	spec := deriveSelectionSpec(dt)

	rows, err := resolveDoltTestRows(sqlCtx, queryist, dt, spec)
	if err != nil {
		return "", err
	}
	return summarizeDoltTestRows(sqlCtx, rows)
}

type selectionSpec struct {
	testsProvided  bool
	groupsProvided bool
	testsWildcard  bool
	groupsWildcard bool
}

func deriveSelectionSpec(dt *dolt_ci.DoltTestStep) selectionSpec {
	testsProvided := len(dt.Tests) > 0
	groupsProvided := len(dt.TestGroups) > 0
	testsWildcard := testsProvided && hasWildcard(dt.Tests)
	groupsWildcard := groupsProvided && hasWildcard(dt.TestGroups)
	return selectionSpec{testsProvided, groupsProvided, testsWildcard, groupsWildcard}
}

func hasWildcard(nodes []yaml.Node) bool {
	if len(nodes) == 1 && strings.TrimSpace(nodes[0].Value) == "*" {
		return true
	}
	return false
}

func resolveDoltTestRows(sqlCtx *sql.Context, queryist cli.Queryist, dt *dolt_ci.DoltTestStep, spec selectionSpec) ([]sql.Row, error) {
	switch {
	case !spec.testsProvided && !spec.groupsProvided:
		return getAllDoltTestRunRows(sqlCtx, queryist)

	case spec.testsProvided && !spec.groupsProvided:
		if spec.testsWildcard {
			return getAllDoltTestRunRows(sqlCtx, queryist)
		}
		return collectRowsForSelectors(sqlCtx, queryist, "test", nodesToValues(dt.Tests))

	case spec.groupsProvided && !spec.testsProvided:
		if spec.groupsWildcard {
			return getAllDoltTestRunRows(sqlCtx, queryist)
		}
		return collectRowsForSelectors(sqlCtx, queryist, "group", nodesToValues(dt.TestGroups))

	default: // both provided
		if spec.testsWildcard && !spec.groupsWildcard {
			// All tests in specified groups
			return collectRowsForSelectors(sqlCtx, queryist, "group", nodesToValues(dt.TestGroups))
		}
		if spec.groupsWildcard && !spec.testsWildcard {
			// Only specified test names across all groups
			return collectRowsForSelectors(sqlCtx, queryist, "test", nodesToValues(dt.Tests))
		}
		// Neither wildcard: intersection
		return collectIntersectionRows(sqlCtx, queryist, nodesToValues(dt.Tests), nodesToValues(dt.TestGroups))
	}
}

func nodesToValues(nodes []yaml.Node) []string {
	vals := make([]string, 0, len(nodes))
	for _, n := range nodes {
		vals = append(vals, n.Value)
	}
	return vals
}

// collectRowsForSelectors fetches rows for each selector using dolt_test_run('<selector>').
// kind should be "test" or "group" to produce specific error messages if an empty result is somehow returned without error.
func collectRowsForSelectors(sqlCtx *sql.Context, queryist cli.Queryist, kind string, selectors []string) ([]sql.Row, error) {
	allRows := make([]sql.Row, 0)
	for _, sel := range selectors {
		rows, err := fetchDoltTestRunRows(sqlCtx, queryist, sel)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			// dolt_test_run should return an error in this scenario; this is a defensive fallback
			if kind == "test" {
				return nil, fmt.Errorf("test '%s' not found", sel)
			}
			return nil, fmt.Errorf("group '%s' not found", sel)
		}
		allRows = append(allRows, rows...)
	}
	return allRows, nil
}

// collectIntersectionRows returns only the rows for the specified tests within each specified group.
// It also verifies that each named test exists within every specified group.
func collectIntersectionRows(sqlCtx *sql.Context, queryist cli.Queryist, testNames, groupNames []string) ([]sql.Row, error) {
	var allRows []sql.Row
	for _, group := range groupNames {
		rows, err := fetchDoltTestRunRows(sqlCtx, queryist, group)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, fmt.Errorf("group '%s' not found", group)
		}
		groupTests := make(map[string]bool)
		for _, r := range rows {
			tName, err := getStringColAsString(sqlCtx, r[0])
			if err != nil {
				return nil, err
			}
			groupTests[tName] = true
		}
		// verify requested tests exist in this group
		for _, t := range testNames {
			if !groupTests[t] {
				return nil, fmt.Errorf("test '%s' not found in group '%s'", t, group)
			}
		}
		// filter rows to only requested tests
		for _, r := range rows {
			tName, err := getStringColAsString(sqlCtx, r[0])
			if err != nil {
				return nil, err
			}
			for _, t := range testNames {
				if tName == t {
					allRows = append(allRows, r)
					break
				}
			}
		}
	}
	return allRows, nil
}

// summarizeDoltTestRows formats and returns details and an aggregated error if any failures occurred.
func summarizeDoltTestRows(sqlCtx *sql.Context, rows []sql.Row) (string, error) {
	details, failures, err := formatDoltTestRows(sqlCtx, rows)
	if err != nil {
		return "", err
	}
	if len(failures) > 0 {
		return details, fmt.Errorf("%s", strings.Join(failures, "; "))
	}
	return details, nil
}

// fetchDoltTestRunRows runs dolt_test_run for the provided selector (test or group value)
func fetchDoltTestRunRows(sqlCtx *sql.Context, queryist cli.Queryist, selector string) ([]sql.Row, error) {
	q := fmt.Sprintf("SELECT * FROM dolt_test_run('%s')", strings.ReplaceAll(selector, "'", "''"))
	return cli.GetRowsForSql(queryist, sqlCtx, q)
}

// getAllDoltTestRunRows runs dolt_test_run() with no arguments to return all rows
func getAllDoltTestRunRows(sqlCtx *sql.Context, queryist cli.Queryist) ([]sql.Row, error) {
	return cli.GetRowsForSql(queryist, sqlCtx, "SELECT * FROM dolt_test_run()")
}

// formatDoltTestRows returns a formatted summary of all tests and a list of failure messages
func formatDoltTestRows(sqlCtx *sql.Context, rows []sql.Row) (string, []string, error) {
    lines := make([]string, 0, len(rows)*2)
	failures := make([]string, 0)
	for _, row := range rows {
		tName, err := getStringColAsString(sqlCtx, row[0])
		if err != nil {
			return "", nil, err
		}
		gName, err := getStringColAsString(sqlCtx, row[1])
		if err != nil {
			return "", nil, err
		}
		status, err := getStringColAsString(sqlCtx, row[3])
		if err != nil {
			return "", nil, err
		}
		message, err := getStringColAsString(sqlCtx, row[4])
		if err != nil {
			return "", nil, err
		}
        statusUpper := strings.ToUpper(status)
        statusColored := statusUpper
        if statusUpper == "PASS" {
            statusColored = color.GreenString(statusUpper)
        } else if statusUpper == "FAIL" {
            statusColored = color.RedString(statusUpper)
        }
        baseLine := fmt.Sprintf("  - test: %s (group: %s) - %s", tName, gName, statusColored)
        lines = append(lines, baseLine)
        if statusUpper != "PASS" {
            if message == "" {
                message = "failed"
            }
            // add separate error line, with error message colored red
            lines = append(lines, fmt.Sprintf("    - error: %s", color.RedString(message)))
            failures = append(failures, fmt.Sprintf("%s: %s", tName, message))
        }
	}
	return strings.Join(lines, "\n"), failures, nil
}
