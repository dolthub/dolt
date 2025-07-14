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
	"context"
	"encoding/json"
	"fmt"
	"testing"

	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
)

// TestJsonSerialization_EncodedBytes tests that we can properly encode JSON data to MySQL's internal encoding.
func TestJsonSerialization_EncodedBytes(t *testing.T) {
	tests := []struct {
		name     string
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
		{
			// String values up to 127 bytes use a one byte length encoding
			name:     "127 byte string value",
			json:     fmt.Sprintf("%q", generateLargeString(127)),
			expected: append([]byte{0xc, 0x7f}, []byte(generateLargeString(127))...),
		},
		{
			// String values over 127 bytes use a two byte length encoding
			name:     "128 byte string value",
			json:     fmt.Sprintf("%q", generateLargeString(128)),
			expected: append([]byte{0xc, 0x80, 0x1}, []byte(generateLargeString(128))...),
		},
		{
			// String values up to 16,383 bytes use a two byte length encoding
			name:     "16,383 byte string value",
			json:     fmt.Sprintf("%q", generateLargeString(16383)),
			expected: append([]byte{0xc, 0xff, 0x7f}, []byte(generateLargeString(16383))...),
		},
		{
			// String values over 16,383 bytes use a three byte length encoding
			name:     "16,384 byte string value",
			json:     fmt.Sprintf("%q", generateLargeString(16384)),
			expected: append([]byte{0xc, 0x80, 0x80, 0x1}, []byte(generateLargeString(16384))...),
		},

		// Arrays
		{
			name: "small array encoding",
			json: `["foo", "bar", true, "baz"]`,
			expected: []byte{0x2, 0x4, 0x0, 0x1c, 0x0, 0xc, 0x10, 0x0, 0xc, 0x14, 0x0, 0x4, 0x1,
				0x0, 0xc, 0x18, 0x0, 0x3, 0x66, 0x6f, 0x6f, 0x3, 0x62, 0x61, 0x72, 0x3, 0x62, 0x61, 0x7a},
		},
		{
			name: "large array encoding",
			json: fmt.Sprintf(`["a","%s","%s","c"]`, generateLargeString(35_000), generateLargeString(35_000)),
			expected: appendByteSlices([]byte{0x3, 0x4, 0x0, 0x0, 0x0, 0x96, 0x11, 0x1, 0x0, 0xc, 0x1c, 0x0, 0x0, 0x0, 0xc, 0x1e, 0x0, 0x0, 0x0, 0xc, 0xd9, 0x88, 0x0, 0x0, 0xc, 0x94, 0x11, 0x1, 0x0, 0x1, 0x61},
				[]byte{0xb8, 0x91, 0x2}, // 35_00 length
				[]byte(generateLargeString(35_000)),
				[]byte{0xb8, 0x91, 0x2}, // 35_00 length
				[]byte(generateLargeString(35_000)),
				[]byte{0x01, 'c'}),
		},

		// Objects
		{
			name: "small object encoding",
			json: `{"foo": "bar", "zap": true}`,
			expected: []byte{0x0, 0x2, 0x0, 0x1c, 0x0, 0x12, 0x0, 0x3, 0x0, 0x15, 0x0, 0x3, 0x0, 0xc,
				0x18, 0x0, 0x4, 0x1, 0x0, 0x66, 0x6f, 0x6f, 0x7a, 0x61, 0x70, 0x3, 0x62, 0x61, 0x72},
		},
		{
			name: "large object encoding",
			json: fmt.Sprintf(`{"a":"%s", "b":"%s"}`, generateLargeString(35_000), generateLargeString(35_000)),
			expected: appendByteSlices([]byte{0x1, 0x2, 0x0, 0x0, 0x0, 0x96, 0x11, 0x1, 0x0, 0x1e, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1f, 0x0, 0x0, 0x0, 0x1, 0x0, 0xc, 0x20, 0x0, 0x0, 0x0, 0xc, 0xdb, 0x88, 0x0, 0x0, 0x61, 0x62},
				[]byte{0xb8, 0x91, 0x2}, // 35_00 length
				[]byte(generateLargeString(35_000)),
				[]byte{0xb8, 0x91, 0x2}, // 35_00 length
				[]byte(generateLargeString(35_000)),
			),
		},
	}

	for _, test := range tests {
		name := test.json
		if test.name != "" {
			name = test.name
		}
		t.Run(name, func(t *testing.T) {
			var jsonDoc any
			require.NoError(t, json.Unmarshal([]byte(test.json), &jsonDoc))
			encoded, err := encodeJsonDoc(context.Background(), gmstypes.JSONDocument{Val: jsonDoc})
			require.NoError(t, err)
			require.Equal(t, test.expected, encoded)
		})
	}
}

// TestJsonSerialization_VitessRoundTrip tests that we can properly encode JSON data to MySQL's internal encoding, then
// decode it with Vitess's logic that parses the binary representation and turns it back into a SQL expression.
func TestJsonSerialization_VitessRoundTrip(t *testing.T) {
	tests := []struct {
		name           string
		json           string
		expected       string
		expectedTypeId byte
		expectedErr    string
	}{
		// Literals
		{
			json:           "true",
			expected:       "'true'",
			expectedTypeId: jsonTypeLiteral,
		},
		{
			json:           "false",
			expected:       "'false'",
			expectedTypeId: jsonTypeLiteral,
		},
		{
			json:           "null",
			expected:       "'null'",
			expectedTypeId: jsonTypeLiteral,
		},

		// Scalars
		{
			json:           `"foo"`,
			expected:       `'"foo"'`,
			expectedTypeId: jsonTypeString,
		},
		{
			json:           "1.0",
			expected:       "'1E+00'",
			expectedTypeId: jsonTypeDouble,
		},
		{
			// String values up to 127 bytes use a one byte length encoding
			name:     "127 byte string value",
			json:     fmt.Sprintf("%q", generateLargeString(127)),
			expected: fmt.Sprintf("'%q'", generateLargeString(127)),
		},
		{
			// String values over 127 bytes use a two byte length encoding
			name:     "128 byte string value",
			json:     fmt.Sprintf("%q", generateLargeString(128)),
			expected: fmt.Sprintf("'%q'", generateLargeString(128)),
		},
		{
			// String values up to 16,383 bytes use a two byte length encoding
			name:     "16,383 byte string value",
			json:     fmt.Sprintf("%q", generateLargeString(16383)),
			expected: fmt.Sprintf("'%q'", generateLargeString(16383)),
		},
		{
			// String values over 16,383 bytes use a three byte length encoding
			name:     "16,384 byte string value",
			json:     fmt.Sprintf("%q", generateLargeString(16384)),
			expected: fmt.Sprintf("'%q'", generateLargeString(16384)),
		},
		{
			// String values over 2,097,151 bytes throw an error
			name:        "2,097,152 bytes string value",
			json:        fmt.Sprintf("%q", generateLargeString(2_097_152)),
			expectedErr: "strings larger than 2,097,151 bytes not supported",
		},

		// Small Arrays
		{
			json:           `["foo", null]`,
			expected:       "JSON_ARRAY('foo',null)",
			expectedTypeId: jsonTypeSmallArray,
		},
		{
			json:           `["foo", "bar", 1, 2, 3]`,
			expected:       "JSON_ARRAY('foo','bar',1E+00,2E+00,3E+00)",
			expectedTypeId: jsonTypeSmallArray,
		},
		{
			json:           `[1.1, [2.2, "foo"], "bar", ["baz", "bash"]]`,
			expected:       "JSON_ARRAY(1.1E+00,JSON_ARRAY(2.2E+00,'foo'),'bar',JSON_ARRAY('baz','bash'))",
			expectedTypeId: jsonTypeSmallArray,
		},
		{
			json:           `[1.1, [2.2, [3.3, ["foo"]]]]`,
			expected:       "JSON_ARRAY(1.1E+00,JSON_ARRAY(2.2E+00,JSON_ARRAY(3.3E+00,JSON_ARRAY('foo'))))",
			expectedTypeId: jsonTypeSmallArray,
		},
		{
			json:           `[1.1, {"foo": ["bar", "baz", "bash"]}, 2.2]`,
			expected:       "JSON_ARRAY(1.1E+00,JSON_OBJECT('foo',JSON_ARRAY('bar','baz','bash')),2.2E+00)",
			expectedTypeId: jsonTypeSmallArray,
		},

		// Small Objects
		{
			json:           `{"foo": "bar", "baz": 1.23}`,
			expected:       "JSON_OBJECT('baz',1.23E+00,'foo','bar')",
			expectedTypeId: jsonTypeSmallObject,
		},
		{
			json:           `{"foo": {"bar": {"baz": {"bash": 1.0}, "boo": 2.0}}}`,
			expected:       "JSON_OBJECT('foo',JSON_OBJECT('bar',JSON_OBJECT('baz',JSON_OBJECT('bash',1E+00),'boo',2E+00)))",
			expectedTypeId: jsonTypeSmallObject,
		},
		{
			json:           `{"foo": ["bar", {"baz": {"bash": [1.123, 2.234]}, "boo": 2.0}]}`,
			expected:       "JSON_OBJECT('foo',JSON_ARRAY('bar',JSON_OBJECT('baz',JSON_OBJECT('bash',JSON_ARRAY(1.123E+00,2.234E+00)),'boo',2E+00)))",
			expectedTypeId: jsonTypeSmallObject,
		},

		// Large Arrays
		{
			name: "large array",
			json: fmt.Sprintf(`[%q, %q, "baz", "bash"]`,
				generateLargeString(33_000), generateLargeString(33_000)),
			expected: fmt.Sprintf(`JSON_ARRAY('%s','%s','baz','bash')`,
				generateLargeString(33_000), generateLargeString(33_000)),
			expectedTypeId: jsonTypeLargeArray,
		},

		// Large Objects
		{
			name: "large object",
			json: fmt.Sprintf(`{"foo": %q, "bar": %q, "zoo": "glorp"}`,
				generateLargeString(33_000), generateLargeString(33_000)),
			expected: fmt.Sprintf(`JSON_OBJECT('bar','%s','foo','%s','zoo','glorp')`,
				generateLargeString(33_000), generateLargeString(33_000)),
			expectedTypeId: jsonTypeLargeObject,
		},
	}

	for _, test := range tests {
		name := test.name
		if test.name == "" {
			name = test.json
		}
		t.Run(name, func(t *testing.T) {
			var jsonDoc any
			require.NoError(t, json.Unmarshal([]byte(test.json), &jsonDoc))
			encoded, err := encodeJsonDoc(context.Background(), gmstypes.JSONDocument{Val: jsonDoc})

			if test.expectedErr != "" {
				require.Equal(t, test.expectedErr, err.Error())
			} else {
				require.NoError(t, err)
				if test.expectedTypeId > 0 {
					require.Equal(t, test.expectedTypeId, encoded[0],
						"Expected type ID (%X) doesn't match actual type ID (%X)", test.expectedTypeId, encoded[0])
				}

				sql, err := mysql.ConvertBinaryJSONToSQL(encoded)
				require.NoError(t, err)
				require.Equal(t, test.expected, sql)
			}
		})
	}
}

func generateLargeString(length uint) (s string) {
	sampleText := "abcdefghijklmnopqrstuvwxyz1234567890"

	for len(s) < int(length) {
		pos := len(sampleText)
		if pos > int(length)-len(s) {
			pos = int(length) - len(s)
		}
		s += sampleText[0:pos]
	}

	return s
}

func appendByteSlices(bytes ...[]byte) []byte {
	length := 0
	for _, byteSlice := range bytes {
		length += len(byteSlice)
	}
	result := make([]byte, length)

	pos := 0
	for _, byteSlice := range bytes {
		pos += copy(result[pos:], byteSlice)
	}
	return result
}
