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

package doltdb

import (
	"github.com/dolthub/go-mysql-server/sql"
	"regexp"
	"strings"
)

// compilePattern takes a dolt_ignore pattern and generate a Regexp that matches against the same table names as the pattern.
func compilePattern(pattern string) (*regexp.Regexp, error) {
	pattern = "^" + regexp.QuoteMeta(pattern) + "$"
	pattern = strings.Replace(pattern, "\\?", ".", -1)
	pattern = strings.Replace(pattern, "\\*", ".*", -1)
	pattern = strings.Replace(pattern, "%", ".*", -1)
	return regexp.Compile(pattern)
}

func patternContainsSpecialCharacters(pattern string) bool {
	return strings.ContainsAny(pattern, "?*%")
}

// MatchTablePattern returns whether a table name matches a table name pattern
func MatchTablePattern(pattern string, table string) (bool, error) {
	re, err := compilePattern(pattern)
	if err != nil {
		return false, err
	}
	return re.MatchString(table), nil
}

// GetMatchingTables returns all tables that match a pattern
func GetMatchingTables(ctx *sql.Context, root RootValue, schemaName string, pattern string) (results []string, err error) {
	// If the pattern doesn't contain any special characters, look up that name.
	if !patternContainsSpecialCharacters(pattern) {
		_, exists, err := root.GetTable(ctx, TableName{Name: pattern, Schema: schemaName})
		if err != nil {
			return nil, err
		}
		if exists {
			return []string{pattern}, nil
		} else {
			return nil, nil
		}
	}
	tables, err := root.GetTableNames(ctx, schemaName, true)
	if err != nil {
		return nil, err
	}
	// Otherwise, iterate over each table on the branch to see if they match.
	for _, tbl := range tables {
		matches, err := MatchTablePattern(pattern, tbl)
		if err != nil {
			return nil, err
		}
		if matches {
			results = append(results, tbl)
		}
	}
	return results, nil
}

// getMoreSpecificPatterns takes a dolt_ignore pattern and generates a Regexp that matches against all patterns
// that are "more specific" than it. (a pattern A is more specific than a pattern B if all names that match A also
// match pattern B, but not vice versa.)
func getMoreSpecificPatterns(lessSpecific string) (*regexp.Regexp, error) {
	pattern := "^" + regexp.QuoteMeta(lessSpecific) + "$"
	// A ? can expand to any character except for a * or %, since that also has special meaning in patterns.

	pattern = strings.Replace(pattern, "\\?", "[^\\*%]", -1)
	pattern = strings.Replace(pattern, "\\*", ".*", -1)
	pattern = strings.Replace(pattern, "%", ".*", -1)
	return regexp.Compile(pattern)
}

// normalizePattern generates an equivalent pattern, such that all equivalent patterns have the same normalized pattern.
// It accomplishes this by replacing all * with %, and removing multiple adjacent %.
// This will get a lot harder to implement once we support escaped characters in patterns.
func normalizePattern(pattern string) string {
	pattern = strings.Replace(pattern, "*", "%", -1)
	for {
		newPattern := strings.Replace(pattern, "%%", "%", -1)
		if newPattern == pattern {
			break
		}
		pattern = newPattern
	}
	return pattern
}
