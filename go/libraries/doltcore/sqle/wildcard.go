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

package sqle

import (
	"regexp"
	"strings"
)

// matchWildcardPattern returns true if |s| fully matches |pattern|. |pattern| may contain '*' wildcards
// which matchWildcardPattern 0 or more characters.
func matchWildcardPattern(pattern string, s string) bool {
	result, _ := regexp.MatchString(wildcardToRegexp(pattern), s)
	return result
}

// containsWildcards return true if the string |s| contains any '*' wildcard characters.
func containsWildcards(s string) bool {
	return strings.Contains(s, "*")
}

// wildcardToRegexp converts a wildcard pattern to a regular expression pattern.
func wildcardToRegexp(pattern string) string {
	components := strings.Split(pattern, "*")
	if len(components) == 1 {
		// if len is 1, there are no *'s, return exact match pattern
		return "^" + pattern + "$"
	}
	var result strings.Builder
	for i, literal := range components {
		// Replace * with .*
		if i > 0 {
			result.WriteString(".*")
		}

		// Quote regex meta characters in the literal text.
		result.WriteString(regexp.QuoteMeta(literal))
	}
	return "^" + result.String() + "$"
}
