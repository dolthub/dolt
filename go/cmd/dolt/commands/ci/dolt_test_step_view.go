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

    "github.com/dolthub/dolt/go/libraries/doltcore/env/actions/dolt_ci"
    dtablefunctions "github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
)

// previewDoltTestStatements returns the SQL queries that would be executed by dolt_test_run
// for the given DoltTestStep selection (groups and tests). We reuse the same selection semantics
// as run-time resolution, but only return the table function invocations for preview.
func previewDoltTestStatements(dt *dolt_ci.DoltTestStep) ([]string, error) {
	selectors := buildPreviewSelectors(dt)
	return makePreviewStatements(selectors), nil
}

// buildPreviewSelectors computes which selectors (test names and group names) to preview based on
// the provided DoltTestStep configuration. Wildcards collapse the corresponding set to a single "*".
func buildPreviewSelectors(dt *dolt_ci.DoltTestStep) []string {
	spec := deriveSelectionSpec(dt)
	testsProvided := len(dt.Tests) > 0
	groupsProvided := len(dt.TestGroups) > 0

	switch {
	case testsProvided && groupsProvided:
		if spec.testsWildcard && !spec.groupsWildcard {
			return nodesToValues(dt.TestGroups)
		}
		if spec.groupsWildcard && !spec.testsWildcard {
			return nodesToValues(dt.Tests)
		}
		if spec.testsWildcard && spec.groupsWildcard {
			return []string{"*"}
		}
		args := append([]string{}, nodesToValues(dt.Tests)...)
		args = append(args, nodesToValues(dt.TestGroups)...)
		return args

	case testsProvided:
		if spec.testsWildcard {
			return []string{"*"}
		}
		return nodesToValues(dt.Tests)

	case groupsProvided:
		if spec.groupsWildcard {
			return []string{"*"}
		}
		return nodesToValues(dt.TestGroups)
	}

	return []string{"*"}
}

// makePreviewStatements constructs human-readable SQL statements that illustrate how
// dolt_test_run would be invoked for each selector. These are for preview output only
// (e.g., `dolt ci view`) and are not executed. We intentionally return the final
// interpolated strings to make the preview easy to read.
func makePreviewStatements(selectors []string) []string {
    fn := (&dtablefunctions.TestsRunTableFunction{}).Name()
    stmts := make([]string, 0, len(selectors))
    for _, s := range selectors {
        // Escape single quotes for display; this is a preview string, not executed SQL.
        esc := strings.ReplaceAll(s, "'", "''")
        stmts = append(stmts, fmt.Sprintf("SELECT * FROM %s('%s')", fn, esc))
    }
    return stmts
}
