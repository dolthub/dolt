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
	"regexp"
	"strings"
)

// compilePattern takes a dolt_ignore pattern and generate a Regexp that matches against the same table names as the pattern.
func compilePattern(pattern string) (*regexp.Regexp, error) {
	pattern = regexp.QuoteMeta(pattern)
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
