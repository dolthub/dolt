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

package tree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnescapeHTMLCodepoints(t *testing.T) {
	// See https://github.com/tianhuil/dolt-json-bug
	cases := []struct {
		name     string
		in       string
		expected string
	}{
		{"no escapes", "{\"d\":\"hello\"}", "{\"d\":\"hello\"}"},
		{"lt", "{\"d\":\"\\u003c\"}", "{\"d\":\"<\"}"},
		{"gt", "{\"d\":\"\\u003e\"}", "{\"d\":\">\"}"},
		{"amp", "{\"d\":\"\\u0026\"}", "{\"d\":\"&\"}"},
		{"all three html", "{\"d\":\"\\u003c\\u003e\\u0026\"}", "{\"d\":\"<>&\"}"},
		{"vertical tab preserved", "{\"d\":\"\\u000b\"}", "{\"d\":\"\\u000b\"}"},
		{"nul preserved", "{\"d\":\"\\u0000\"}", "{\"d\":\"\\u0000\"}"},
		{"three control chars", "{\"d\":\"\\u000b\\u000b\\u000b\"}", "{\"d\":\"\\u000b\\u000b\\u000b\"}"},
		{"control then html", "{\"a\":\"\\u000b\\u003c\"}", "{\"a\":\"\\u000b<\"}"},
		{"html then control", "{\"a\":\"\\u003c\\u000b\"}", "{\"a\":\"<\\u000b\"}"},
		{"escaped backslash before u", "{\"d\":\"\\\\u003c\"}", "{\"d\":\"\\\\u003c\"}"},
		{"truncated unicode escape", "ab\\u0", "ab\\u0"},
		{"lone trailing backslash", "ab\\", "ab\\"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := unescapeHTMLCodepoints([]byte(tc.in))
			require.Equal(t, tc.expected, string(got))
		})
	}
}
