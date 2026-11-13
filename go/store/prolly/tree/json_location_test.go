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

func TestToMysqlJsonPathQuotesKeys(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		want string
	}{
		{"plain key", []string{"a"}, `$.a`},
		{"identifier chars", []string{"_a$1", "b"}, `$._a$1.b`},
		{"dot in key", []string{"a.b"}, `$."a.b"`},
		{"dot in nested key", []string{"x", "a.b"}, `$.x."a.b"`},
		{"star key", []string{"*"}, `$."*"`},
		{"bracket in key", []string{"a[0]"}, `$."a[0]"`},
		{"space in key", []string{"a b"}, `$."a b"`},
		{"leading digit", []string{"1a"}, `$."1a"`},
		{"quote in key", []string{`a"b`}, `$."a\"b"`},
		{"backslash in key", []string{`a\b`}, `$."a\\b"`},
		{"empty key", []string{""}, `$.""`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc := newRootLocation()
			for _, k := range tt.keys {
				loc.appendObjectKey([]byte(k))
			}
			require.Equal(t, tt.want, loc.toMysqlJsonPath())
		})
	}
}
