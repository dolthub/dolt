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
	"fmt"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
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
			doc:  sql.MustJSON(`null`),
		},
		{
			name: "boolean",
			doc:  sql.MustJSON(`false`),
		},
		{
			name: "string",
			doc:  sql.MustJSON(`"lorem ipsum"`),
		},
		{
			name: "number",
			doc:  sql.MustJSON(`2.71`),
		},
		{
			name: "type homogenous object",
			doc:  sql.MustJSON(`{"a": 2, "b": 3, "c": 4}`),
		},
		{
			name: "type heterogeneous object",
			doc:  sql.MustJSON(`{"a": 2, "b": "two", "c": false}`),
		},
		{
			name: "homogenous array",
			doc:  sql.MustJSON(`[1, 2, 3]`),
		},
		{
			name: "heterogeneous array",
			doc:  sql.MustJSON(`[1, "two", false]`),
		},
		{
			name: "nested",
			doc:  sql.MustJSON(`[{"a":1}, {"b":2}, null, [false, 3.14, [], {"c": [0]}], ""]`),
		},
	}

	ctx := sql.NewEmptyContext()
	vrw := types.NewMemoryValueStore()

	for _, test := range tests {

		t.Run(test.name, func(t *testing.T) {
			nomsVal, err := NomsJSONFromJSONValue(ctx, vrw, test.doc)
			assert.NoError(t, err)

			// sql.JSONDocument -> NomsJSON -> sql.JSONDocument
			jsDoc, err := nomsVal.Unmarshall(ctx)
			assert.NoError(t, err)
			assert.Equal(t, test.doc.Val, jsDoc.Val)

			// sql.JSONDocument -> NomsJSON -> string -> sql.JSONDocument
			str, err := nomsVal.ToString(ctx)
			assert.NoError(t, err)

			var val interface{}
			err = js.Unmarshal([]byte(str), &val)
			assert.NoError(t, err)

			jsDoc = sql.JSONDocument{Val: val}
			assert.Equal(t, test.doc.Val, jsDoc.Val)
		})
	}

}

func TestJSONCompare(t *testing.T) {
	tests := []struct {
		left  string
		right string
		cmp   int
	}{
		// type precedence hierarchy: BOOLEAN, ARRAY, OBJECT, STRING, DOUBLE, NULL
		{`true`, `[0]`, 1},
		{`[0]`, `{"a": 0}`, 1},
		{`{"a": 0}`, `"a"`, 1},
		{`"a"`, `0`, 1},
		{`0`, `null`, 1},

		// null
		{`null`, `0`, -1},
		{`0`, `null`, 1},
		{`null`, `null`, 0},

		// boolean
		{`true`, `false`, 1},
		{`true`, `true`, 0},
		{`false`, `false`, 0},

		// strings
		{`"A"`, `"B"`, -1},
		{`"A"`, `"A"`, 0},
		{`"C"`, `"B"`, 1},

		// numbers
		{`0`, `0.0`, 0},
		{`0`, `-1`, 1},
		{`0`, `3.14`, -1},

		// TODO(andy): ordering NomsJSON objects and arrays
		//  differs from sql.JSONDocument
		//  MySQL doesn't specify order of unequal objects/arrays

		// arrays
		{`[1,2]`, `[1,2]`, 0},
		// deterministic array ordering by hash
		{`[1,2]`, `[1,9]`, 1},

		// objects
		{`{"a": 0}`, `{"a": 0}`, 0},
		// deterministic object ordering by hash
		{`{"a": 1}`, `{"a": 0}`, 1},
	}

	ctx := sql.NewEmptyContext()
	for _, test := range tests {
		name := fmt.Sprintf("%v_%v__%d", test.left, test.right, test.cmp)
		t.Run(name, func(t *testing.T) {
			left, right := MustNomsJSON(test.left), MustNomsJSON(test.right)
			cmp, err := left.Compare(ctx, right)
			require.NoError(t, err)
			assert.Equal(t, test.cmp, cmp)
		})
	}
}

func TestJSONStructuralSharing(t *testing.T) {
	// runs test with avg chunk size of 256 bytes
	types.TestWithSmallChunks(func() {
		sb := strings.Builder{}
		sb.WriteString(`{"0000":"0000"`)
		i := 1
		const jsonSize = 100
		for i < jsonSize {
			sb.WriteString(fmt.Sprintf(`,"%04d":"%04d"`, i, i))
			i++
		}
		sb.WriteRune('}')

		ctx := context.Background()
		ts := &chunks.TestStorage{}
		db := datas.NewDatabase(ts.NewViewWithDefaultFormat())

		val := MustNomsJSONWithVRW(db, sb.String())

		err := db.Flush(ctx)
		require.NoError(t, err)
		before := ts.Len()

		// assert |val| is chunked given that
		// Database.Flush() writes a chunk itself
		assert.GreaterOrEqual(t, before, 2)

		i = 0
		const tuples = 20
		for i < tuples {
			tup, err := types.NewTuple(types.Format_Default, types.Int(i), types.JSON(val))
			require.NoError(t, err)
			_, err = db.WriteValue(ctx, tup)
			require.NoError(t, err)
			i++
		}

		err = db.Flush(ctx)
		require.NoError(t, err)
		err = db.(datas.GarbageCollector).GC(ctx)
		require.NoError(t, err)
		after := ts.Len()

		// extras chunks are sometimes written
		const errMargin = 5

		assert.Greater(t, before+tuples+errMargin, after)
	})
}
