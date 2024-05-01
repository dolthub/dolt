// Copyright 2024 Dolthub, Inc.
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

package types

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUnescapeHTMLCodepoints(t *testing.T) {
	testCases := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{
			name:     "Unescape <",
			input:    []byte("\\u003c"),
			expected: []byte("<"),
		},
		{
			name:     "Unescape >",
			input:    []byte("\\u003e"),
			expected: []byte(">"),
		},
		{
			name:     "Unescape &",
			input:    []byte("\\u0026"),
			expected: []byte("&"),
		},
		{
			name:     "Don't unescape other codepoints",
			input:    []byte("\\u00ff"),
			expected: []byte("\\u00ff"),
		},
		{
			name:     "Escape multiple codepoints",
			input:    []byte("\\u003c\\u003e\\u0026"),
			expected: []byte("<>&"),
		},
		{
			name:     "Don't escape if the \\ is escaped",
			input:    []byte("\\\\u003c"),
			expected: []byte("\\\\u003c"),
		},
		{
			name:     "Escape codepoints w/ surrounding text",
			input:    []byte("A\\u003cB"),
			expected: []byte("A<B")},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := UnescapeHTMLCodepoints(testCase.input)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}
