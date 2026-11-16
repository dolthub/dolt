// Copyright 2026 Dolthub, Inc.
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

package resolve

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSplitSearchPath asserts the same splitting Postgres performs on a search_path setting, as observed from
// PostgreSQL 16. Each expectation below matches what `SELECT current_schemas(false)` reports after the
// corresponding value has been installed with set_config('search_path', <input>, false).
func TestSplitSearchPath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty value has no elements",
			input:    "",
			expected: nil,
		},
		{
			name:     "whitespace only has no elements",
			input:    "   ",
			expected: nil,
		},
		{
			name:     "single bare element",
			input:    "public",
			expected: []string{"public"},
		},
		{
			name:     "bare elements are lower cased, like unquoted identifiers",
			input:    "PUBLIC, MiXeD",
			expected: []string{"public", "mixed"},
		},
		{
			name:     "the default value keeps $user as its own element",
			input:    `"$user", public`,
			expected: []string{"$user", "public"},
		},
		{
			name:     "quoting preserves case",
			input:    `"MixedCase", public`,
			expected: []string{"MixedCase", "public"},
		},
		{
			name:     "whitespace around elements is insignificant",
			input:    "  public  ,  public2  ",
			expected: []string{"public", "public2"},
		},
		{
			name:     "a quoted element may contain a space",
			input:    `"with space",public`,
			expected: []string{"with space", "public"},
		},
		{
			name:     "a comma inside quotes does not separate elements",
			input:    `"a, b"`,
			expected: []string{"a, b"},
		},
		{
			name:     "a doubled quote inside a quoted element is a literal quote",
			input:    `"with""quote", public`,
			expected: []string{`with"quote`, "public"},
		},
		{
			name:     "an empty quoted element is an element naming no schema",
			input:    `""`,
			expected: []string{""},
		},
		{
			name:     "quoted and bare elements mix",
			input:    `pg_catalog, "MixedCase", public`,
			expected: []string{"pg_catalog", "MixedCase", "public"},
		},
		{
			name:     "digits are not special",
			input:    "1, public",
			expected: []string{"1", "public"},
		},
		{
			name:     "a lone quoted $user is the placeholder element",
			input:    `"$user"`,
			expected: []string{"$user"},
		},
		{
			name:     "an unquoted $user is the placeholder element too",
			input:    "$user, public",
			expected: []string{"$user", "public"},
		},
		{
			name:     "an unterminated quote runs to the end of the value",
			input:    `"unterminated`,
			expected: []string{"unterminated"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, SplitSearchPath(test.input))
		})
	}
}
