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

package json

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

func TestJSONLWriterViaHeaderConfig(t *testing.T) {
	colColl := schema.NewColCollection(
		schema.Column{
			Name:       "id",
			Tag:        0,
			Kind:       types.IntKind,
			IsPartOfPK: true,
			TypeInfo:   typeinfo.Int64Type,
		},
		schema.Column{
			Name:       "payload",
			Tag:        1,
			Kind:       types.JSONKind,
			IsPartOfPK: false,
			TypeInfo:   typeinfo.JSONType,
		},
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)

	var buf bytes.Buffer
	wr, err := NewJSONWriterWithHeader(iohelp.NopWrCloser(&buf), sch, "", "\n", "\n")
	require.NoError(t, err)

	sqlCtx := sql.NewEmptyContext()
	require.NoError(t, wr.WriteSqlRow(sqlCtx, sql.Row{int64(0), gmstypes.MustJSON(`{"a": 1}`)}))
	require.NoError(t, wr.WriteSqlRow(sqlCtx, sql.Row{int64(1), gmstypes.MustJSON(`[1, 2]`)}))
	require.NoError(t, wr.Close(context.Background()))

	out := buf.String()
	require.True(t, strings.HasSuffix(out, "\n"))

	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	require.Len(t, lines, 2)

	var row0 map[string]any
	require.NoError(t, stdjson.Unmarshal([]byte(lines[0]), &row0))
	require.Equal(t, float64(0), row0["id"])

	payload0, ok := row0["payload"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(1), payload0["a"])

	var row1 map[string]any
	require.NoError(t, stdjson.Unmarshal([]byte(lines[1]), &row1))
	require.Equal(t, float64(1), row1["id"])

	payload1, ok := row1["payload"].([]any)
	require.True(t, ok)
	require.Equal(t, []any{float64(1), float64(2)}, payload1)
}

func TestJSONLWriterEmptyWritesNothing(t *testing.T) {
	colColl := schema.NewColCollection(
		schema.Column{
			Name:       "id",
			Tag:        0,
			Kind:       types.IntKind,
			IsPartOfPK: true,
			TypeInfo:   typeinfo.Int64Type,
		},
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)

	var buf bytes.Buffer
	wr, err := NewJSONWriterWithHeader(iohelp.NopWrCloser(&buf), sch, "", "\n", "\n")
	require.NoError(t, err)
	require.NoError(t, wr.Close(context.Background()))
	require.Equal(t, "", buf.String())
}

func TestJSONWriterPreservesNumberKinds(t *testing.T) {
	ctx := sql.NewEmptyContext()
	sch, err := schema.SchemaFromCols(schema.NewColCollection(schema.Column{
		Name: "payload", Tag: 0, Kind: types.JSONKind, TypeInfo: typeinfo.JSONType,
	}))
	require.NoError(t, err)
	for _, sqlSchema := range []bool{false, true} {
		var buf bytes.Buffer
		var writer *RowWriter
		if sqlSchema {
			writer, err = NewJSONSqlWriter(iohelp.NopWrCloser(&buf), sql.Schema{&sql.Column{Name: "payload", Type: gmstypes.JSON}})
		} else {
			writer, err = NewJSONWriter(iohelp.NopWrCloser(&buf), sch)
		}
		require.NoError(t, err)
		doc := gmstypes.MustJSON(`{"integer":42,"double":42.0,"large":18446744073709551615}`)
		require.NoError(t, writer.WriteSqlRow(ctx, sql.Row{doc}))
		require.NoError(t, writer.Close(ctx))
		var decoded any
		require.NoError(t, gmstypes.JsonUnmarshal(buf.Bytes(), &decoded))
		payload := decoded.(map[string]any)["rows"].([]any)[0].(map[string]any)["payload"]
		require.Equal(t, doc.Val, payload)
	}
}
