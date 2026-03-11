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
	"io"
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func testGoodJSONL(t *testing.T, getReader func(types.ValueReadWriter, schema.Schema) (sqlTableReaderWithVerify, error)) {
	colColl := schema.NewColCollection(
		schema.Column{
			Name:       "id",
			Tag:        0,
			Kind:       types.IntKind,
			IsPartOfPK: true,
			TypeInfo:   typeinfo.Int64Type,
		},
		schema.Column{
			Name:       "first name",
			Tag:        1,
			Kind:       types.StringKind,
			IsPartOfPK: false,
			TypeInfo:   typeinfo.StringDefaultType,
		},
		schema.Column{
			Name:       "last name",
			Tag:        2,
			Kind:       types.StringKind,
			IsPartOfPK: false,
			TypeInfo:   typeinfo.StringDefaultType,
		},
	)

	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)

	sqlSch, err := sqlutil.FromDoltSchema("", "", sch)
	require.NoError(t, err)

	vrw := types.NewMemoryValueStore()
	reader, err := getReader(vrw, sch)
	require.NoError(t, err)

	verifySchema, err := reader.VerifySchema(sch)
	require.NoError(t, err)
	assert.True(t, verifySchema)

	var rows []sql.Row
	for {
		r, err := reader.ReadSqlRow(context.Background())
		if err == io.EOF {
			break
		} else {
			require.NoError(t, err)
		}
		rows = append(rows, r)
	}

	expectedRows := []sql.Row{
		{0, "tim", "sehn"},
		{1, "brian", "hendriks"},
	}

	assert.Equal(t, enginetest.WidenRows(t, sqlSch.Schema, expectedRows), rows)
}

// sqlTableReaderWithVerify is the minimal interface needed for the shared test helper.
type sqlTableReaderWithVerify interface {
	ReadSqlRow(ctx context.Context) (sql.Row, error)
	VerifySchema(sch schema.Schema) (bool, error)
}

func TestJSONLReader(t *testing.T) {
	testJSONL := `{"id":0,"first name":"tim","last name":"sehn"}
{"id":1,"first name":"brian","last name":"hendriks"}`

	fs := filesys.EmptyInMemFS("/")
	require.NoError(t, fs.WriteFile("file.jsonl", []byte(testJSONL), os.ModePerm))

	testGoodJSONL(t, func(vrw types.ValueReadWriter, sch schema.Schema) (sqlTableReaderWithVerify, error) {
		return OpenJSONLReader(vrw, "file.jsonl", fs, sch)
	})
}

func TestJSONLReaderSkipsBlankLines(t *testing.T) {
	testJSONL := `{"id":0,"first name":"tim","last name":"sehn"}


{"id":1,"first name":"brian","last name":"hendriks"}
`

	fs := filesys.EmptyInMemFS("/")
	require.NoError(t, fs.WriteFile("file.jsonl", []byte(testJSONL), os.ModePerm))

	testGoodJSONL(t, func(vrw types.ValueReadWriter, sch schema.Schema) (sqlTableReaderWithVerify, error) {
		return OpenJSONLReader(vrw, "file.jsonl", fs, sch)
	})
}

func TestJSONLReaderBadJsonIncludesLineNumber(t *testing.T) {
	testJSONL := `{"id":0,"first name":"tim","last name":"sehn"}
bad
{"id":1,"first name":"brian","last name":"hendriks"}`

	fs := filesys.EmptyInMemFS("/")
	require.NoError(t, fs.WriteFile("file.jsonl", []byte(testJSONL), os.ModePerm))

	colColl := schema.NewColCollection(
		schema.Column{Name: "id", Tag: 0, Kind: types.IntKind, IsPartOfPK: true, TypeInfo: typeinfo.Int64Type},
		schema.Column{Name: "first name", Tag: 1, Kind: types.StringKind, IsPartOfPK: false, TypeInfo: typeinfo.StringDefaultType},
		schema.Column{Name: "last name", Tag: 2, Kind: types.StringKind, IsPartOfPK: false, TypeInfo: typeinfo.StringDefaultType},
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)

	vrw := types.NewMemoryValueStore()
	reader, err := OpenJSONLReader(vrw, "file.jsonl", fs, sch)
	require.NoError(t, err)

	_, err = reader.ReadSqlRow(context.Background())
	require.NoError(t, err)

	_, err = reader.ReadSqlRow(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid JSON at line 2")
}

func TestJSONLReaderRejectsNonObject(t *testing.T) {
	testJSONL := `[]`

	fs := filesys.EmptyInMemFS("/")
	require.NoError(t, fs.WriteFile("file.jsonl", []byte(testJSONL), os.ModePerm))

	colColl := schema.NewColCollection(
		schema.Column{Name: "id", Tag: 0, Kind: types.IntKind, IsPartOfPK: true, TypeInfo: typeinfo.Int64Type},
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)

	vrw := types.NewMemoryValueStore()
	reader, err := OpenJSONLReader(vrw, "file.jsonl", fs, sch)
	require.NoError(t, err)

	_, err = reader.ReadSqlRow(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected JSON object at line 1")
}

func TestJSONLReaderBOMHandlingUTF8(t *testing.T) {
	testJSONL := `{"id":0,"first name":"tim","last name":"sehn"}
{"id":1,"first name":"brian","last name":"hendriks"}`

	bs := bytes.NewBuffer([]byte(testJSONL))
	reader := transform.NewReader(bs, unicode.UTF8BOM.NewEncoder())

	testGoodJSONL(t, func(vrw types.ValueReadWriter, sch schema.Schema) (sqlTableReaderWithVerify, error) {
		return NewJSONLReader(vrw, io.NopCloser(reader), sch)
	})
}
