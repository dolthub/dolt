// Copyright 2021 Dolthub, Inc.
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

package json

import (
	"context"
	js "encoding/json"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/types"
)

func TestJSONValueMarshallingRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		doc  sql.JSONDocument
	}{
		{
			name: "smoke",
			doc:  sql.MustJSON(`[]`),
		},
		{
			name: "null",
			doc: sql.MustJSON(`null`),
		},
		{
			name: "boolean",
			doc: sql.MustJSON(`false`),
		},
		{
			name: "string",
			doc: sql.MustJSON(`"lorem ipsum"`),
		},
		{
			name: "number",
			doc: sql.MustJSON(`2.71`),
		},
		{
			name: "type homogenous object",
			doc: sql.MustJSON(`{"a": 2, "b": 3, "c": 4}`),
		},
		{
			name: "type heterogeneous object",
			doc:  sql.MustJSON(`{"a": 2, "b": "two", "c": false}`),
		},
		{
			name: "homogenous array",
			doc: sql.MustJSON(`[1, 2, 3]`),
		},
		{
			name: "heterogeneous array",
			doc:  sql.MustJSON(`[1, "two", false]`),
		},
		{
			name: "nested",
			doc: sql.MustJSON(`[{"a":1}, {"b":2}, null, [false, 3.14, [], {"c": [0]}], ""]`),
		},
	}

	ctx := context.Background()
	vrw := types.NewMemoryValueStore()

	for _, test := range tests {

		t.Run(test.name, func(t *testing.T) {
			nomsVal, err := NomsJSONFromJSONValue(ctx, vrw, test.doc)
			assert.NoError(t, err)

			// sql.JSONDocument -> NomsJSON -> sql.JSONDocument
			jsDoc, err := nomsVal.Unmarshall()
			assert.NoError(t, err)
			assert.Equal(t, test.doc.Val, jsDoc.Val)

			// sql.JSONDocument -> NomsJSON -> string -> sql.JSONDocument
			str, err := nomsVal.ToString()
			assert.NoError(t, err)

			var val interface{}
			err = js.Unmarshal([]byte(str), &val)
			assert.NoError(t, err)

			jsDoc = sql.JSONDocument{Val: val}
			assert.Equal(t, test.doc.Val, jsDoc.Val)
		})
	}

}
