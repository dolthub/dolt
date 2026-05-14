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

package actions

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCleanCommitMessage covers the [CleanupStrip] rules.
// See https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---cleanupltmodegt.
func TestCleanCommitMessage(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"trailing spaces stripped", "hello world   ", "hello world"},
		{"leading spaces preserved", "   hello world", "   hello world"},
		{"padded both sides", "   hello world   ", "   hello world"},
		{"trailing tab stripped leading tab kept", "\thello\t", "\thello"},
		{"internal spaces kept", "hello   world", "hello   world"},

		{"whitespace only spaces", "   ", ""},
		{"whitespace only tabs", "\t\t", ""},
		{"newlines only", "\n\n", ""},
		{"empty string", "", ""},

		{"multiline trailing spaces", "line1   \nline2   ", "line1\nline2"},
		{"multiline leading spaces preserved", "  line1\n  line2", "  line1\n  line2"},
		{"leading blank lines dropped", "\n\nhello", "hello"},
		{"trailing blank lines dropped", "hello\n\n", "hello"},
		{"surrounding blank lines dropped", "\n\nhello\n\n", "hello"},
		{"single internal blank preserved", "subject\n\nbody", "subject\n\nbody"},
		{"multiple internal blanks collapsed", "subject\n\n\n\nbody", "subject\n\nbody"},
		{"spaces-only interior line becomes blank", "hello\n   \nworld", "hello\n\nworld"},
		{"trailing carriage return stripped", "hello\r\nworld\r\n", "hello\nworld"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, cleanCommitMessage(tt.input))
		})
	}
}

// TestApplyCleanup verifies that CleanupStrip cleans the message and CleanupVerbatim passes it
// through unchanged.
func TestApplyCleanup(t *testing.T) {
	dirty := "hello   \n\n\nworld   "
	require.Equal(t, "hello\n\nworld", applyCleanup(CleanupStrip, dirty))
	require.Equal(t, dirty, applyCleanup(CleanupVerbatim, dirty))
}
