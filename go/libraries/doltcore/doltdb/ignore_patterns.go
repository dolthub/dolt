// Copyright 2023 Dolthub, Inc.
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
	"fmt"
	"regexp"
	"strings"
)

type ignorePattern struct {
	pattern string
	ignore  bool
}

// compilePattern takes a dolt_ignore pattern and generate a Regexp that matches against the same table names as the pattern.
func compilePattern(pattern string) (*regexp.Regexp, error) {
	pattern = "^" + regexp.QuoteMeta(pattern) + "$"
	pattern = strings.Replace(pattern, "\\?", ".", -1)
	pattern = strings.Replace(pattern, "\\*", ".*", -1)
	return regexp.Compile(pattern)
}

// getMoreSpecificPatterns takes a dolt_ignore pattern and generates a Regexp that matches against all patterns
// that are "more specific" than it. (a pattern A is more specific than a pattern B if all names that match A also
// match pattern B, but not vice versa.)
func getMoreSpecificPatterns(lessSpecific string) (*regexp.Regexp, error) {
	pattern := regexp.QuoteMeta(lessSpecific)
	// A ? can expand to any character except for a *, since that also has special meaning in patterns.
	pattern = strings.Replace(pattern, "\\?", "[^\\*]", -1)
	pattern = strings.Replace(pattern, "\\*", ".*", -1)
	return regexp.Compile(pattern)
}

func resolveConflictingPatterns(trueMatches, falseMatches []string, tableName string) (bool, error) {
	trueMatchesToRemove := map[string]struct{}{}
	falseMatchesToRemove := map[string]struct{}{}
	for _, trueMatch := range trueMatches {
		trueMatchRegExp, err := getMoreSpecificPatterns(trueMatch)
		if err != nil {
			return false, err
		}
		for _, falseMatch := range falseMatches {
			if trueMatchRegExp.MatchString(falseMatch) {
				trueMatchesToRemove[trueMatch] = struct{}{}
			}
		}
	}
	for _, falseMatch := range falseMatches {
		falseMatchRegExp, err := getMoreSpecificPatterns(falseMatch)
		if err != nil {
			return false, err
		}
		for _, trueMatch := range trueMatches {
			if falseMatchRegExp.MatchString(trueMatch) {
				falseMatchesToRemove[falseMatch] = struct{}{}
			}
		}
	}
	if len(trueMatchesToRemove) == len(trueMatches) {
		return false, nil
	}
	if len(falseMatchesToRemove) == len(falseMatches) {
		return true, nil
	}
	return false, fmt.Errorf("dolt_ignore has multiple conflicting rules for %s", tableName)
}

func isTableNameIgnored(patterns []ignorePattern, tableName string) (bool, error) {
	trueMatches := []string{}
	falseMatches := []string{}
	for _, patternIgnore := range patterns {
		pattern := patternIgnore.pattern
		ignore := patternIgnore.ignore
		patternRegExp, err := compilePattern(pattern)
		if err != nil {
			return false, err
		}
		if patternRegExp.MatchString(tableName) {
			if ignore {
				trueMatches = append(trueMatches, tableName)
			} else {
				falseMatches = append(falseMatches, tableName)
			}
		}
	}
	if len(trueMatches) == 0 {
		return false, nil
	}
	if len(falseMatches) == 0 {
		return true, nil
	}
	// The table name matched both positive and negative patterns.
	// More specific patterns override less specific patterns.
	ignoreTable, err := resolveConflictingPatterns(trueMatches, falseMatches, tableName)
	if err != nil {
		return false, err
	}
	return ignoreTable, nil
}
