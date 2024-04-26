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

package binlogreplication

import (
	"encoding/json"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestJsonSerialization_EncodedBytes tests that we can properly encode JSON data to MySQL's internal encoding.
func TestJsonSerialization_EncodedBytes(t *testing.T) {
	tests := []struct {
		json     string
		expected []byte
	}{
		// Literals
		{
			json:     "true",
			expected: []byte{0x4, 0x1},
		},
		{
			json:     "false",
			expected: []byte{0x4, 0x2},
		},
		{
			json:     "null",
			expected: []byte{0x4, 0x0},
		},

		// Scalars
		{
			json:     `"foo"`,
			expected: []byte{0xc, 0x3, 0x66, 0x6f, 0x6f},
		},
		{
			json:     "1.0",
			expected: []byte{0xb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f},
		},

		// Small Arrays

		// Small Objects

		// Large Arrays

		// Large Objects
	}

	for _, test := range tests {
		t.Run(test.json, func(t *testing.T) {
			var jsonDoc any
			require.NoError(t, json.Unmarshal([]byte(test.json), &jsonDoc))
			encoded, err := encodeJsonDoc(jsonDoc)
			require.NoError(t, err)
			require.Equal(t, test.expected, encoded)
		})
	}
}

// TestJsonSerialization_VitessRoundTrip tests that we can properly encode JSON data to MySQL's internal encoding, then
// decode it with Vitess's logic that parses the binary representation and turns it back into a SQL expression.
func TestJsonSerialization_VitessRoundTrip(t *testing.T) {
	tests := []struct {
		json     string
		expected string
	}{
		// Literals
		{
			json:     "true",
			expected: "'true'",
		},
		{
			json:     "false",
			expected: "'false'",
		},
		{
			json:     "null",
			expected: "'null'",
		},

		// Scalars
		{
			json:     `"foo"`,
			expected: `'"foo"'`,
		},
		{
			json:     "1.0",
			expected: "'1E+00'",
		},

		// Small Arrays
		{
			json:     `["foo", "bar", 1, 2, 3]`,
			expected: "JSON_ARRAY('foo','bar',1E+00,2E+00,3E+00)",
		},
		{
			json:     `[1.1, [2.2, "foo"], "bar", ["baz", "bash"]]`,
			expected: "JSON_ARRAY(1.1E+00,JSON_ARRAY(2.2E+00,'foo'),'bar',JSON_ARRAY('baz','bash'))",
		},
		{
			json:     `[1.1, [2.2, [3.3, ["foo"]]]]`,
			expected: "JSON_ARRAY(1.1E+00,JSON_ARRAY(2.2E+00,JSON_ARRAY(3.3E+00,JSON_ARRAY('foo'))))",
		},
		{
			json:     `[1.1, {"foo": ["bar", "baz", "bash"]}, 2.2]`,
			expected: "JSON_ARRAY(1.1E+00,JSON_OBJECT('foo',JSON_ARRAY('bar','baz','bash')),2.2E+00)",
		},

		// Small Objects
		{
			json:     `{"foo": "bar", "baz": 1.23}`,
			expected: "JSON_OBJECT('baz',1.23E+00,'foo','bar')",
		},
		{
			json:     `{"foo": {"bar": {"baz": {"bash": 1.0}, "boo": 2.0}}}`,
			expected: "JSON_OBJECT('foo',JSON_OBJECT('bar',JSON_OBJECT('baz',JSON_OBJECT('bash',1E+00),'boo',2E+00)))",
		},
		{
			json:     `{"foo": ["bar", {"baz": {"bash": [1.123, 2.234]}, "boo": 2.0}]}`,
			expected: "JSON_OBJECT('foo',JSON_ARRAY('bar',JSON_OBJECT('baz',JSON_OBJECT('bash',JSON_ARRAY(1.123E+00,2.234E+00)),'boo',2E+00)))",
		},

		// Large Arrays

		// Large Objects
	}

	for _, test := range tests {
		t.Run(test.json, func(t *testing.T) {
			var jsonDoc any
			require.NoError(t, json.Unmarshal([]byte(test.json), &jsonDoc))
			encoded, err := encodeJsonDoc(jsonDoc)
			require.NoError(t, err)

			sql, err := mysql.ConvertBinaryJSONToSQL(encoded)
			require.NoError(t, err)
			require.Equal(t, test.expected, sql)
		})
	}
}
