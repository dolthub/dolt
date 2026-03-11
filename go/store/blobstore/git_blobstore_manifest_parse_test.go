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

package blobstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseManifestTableNames(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "v5 with two tables",
			input:    "5:__DOLT__:lockhash:roothash:gcgenhash:table1:10:table2:20",
			expected: []string{"table1", "table2"},
		},
		{
			name:     "v5 with one table",
			input:    "5:__DOLT__:lockhash:roothash:gcgenhash:tablename:5",
			expected: []string{"tablename"},
		},
		{
			name:     "v5 with no tables",
			input:    "5:__DOLT__:lockhash:roothash:gcgenhash",
			expected: []string{},
		},
		{
			name:     "v4 with two tables",
			input:    "4:__DOLT__:lockhash:roothash:table1:10:table2:20",
			expected: []string{"table1", "table2"},
		},
		{
			name:     "v4 with no tables",
			input:    "4:__DOLT__:lockhash:roothash",
			expected: []string{},
		},
		{
			name:     "empty input",
			input:    "",
			expected: nil,
		},
		{
			name:     "unknown version",
			input:    "3:something:else",
			expected: nil,
		},
		{
			name:     "v5 odd spec count returns nil",
			input:    "5:__DOLT__:lockhash:roothash:gcgenhash:table1",
			expected: nil,
		},
		{
			name:     "v5 with many tables",
			input:    "5:nbf:lock:root:gc:a:1:b:2:c:3:d:4:e:5:f:6",
			expected: []string{"a", "b", "c", "d", "e", "f"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseManifestTableNames([]byte(tt.input))
			if tt.expected == nil {
				assert.Nil(t, result, "expected nil but got %v", result)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseManifestTableNamesRealisticV5(t *testing.T) {
	// Build a realistic V5 manifest with proper hash-length names
	hashA := strings.Repeat("a", 32)
	hashB := strings.Repeat("b", 32)
	manifest := "5:__DOLT__:" + strings.Repeat("0", 32) + ":" + strings.Repeat("1", 32) + ":" + strings.Repeat("2", 32) + ":" + hashA + ":100:" + hashB + ":200"

	names := parseManifestTableNames([]byte(manifest))
	assert.Equal(t, []string{hashA, hashB}, names)
}
