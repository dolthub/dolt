// Copyright 2019 Dolthub, Inc.
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
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func TestReader(t *testing.T) {
	testJSON := `{
		"rows": [
			 {
			   "id": 0,
			   "first name": "tim",
			   "last name": "sehn"
			},
			{
			   "id": 1,
			   "first name": "brian",
			   "last name": "hendriks"
			}
		]
	}`

	fs := filesys.EmptyInMemFS("/")
	require.NoError(t, fs.WriteFile("file.json", []byte(testJSON)))

	sch := sql.Schema{
		&sql.Column{
			Name:       "id",
			Type:       sql.Int64,
			PrimaryKey: true,
		},
		&sql.Column{
			Name:       "first name",
			Type:       sql.Text,
			PrimaryKey: false,
		},
		&sql.Column{
			Name:       "last name",
			Type:       sql.Text,
			PrimaryKey: false,
		},
	}

	reader, err := OpenJSONReader("file.json", fs, sql.NewPrimaryKeySchema(sch))
	require.NoError(t, err)

	for i, col := range reader.GetSqlSchema().Schema {
		assert.True(t, col.Equals(sch[i]))
	}

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

	require.Equal(t, enginetest.WidenRows(sch, expectedRows), enginetest.WidenRows(sch, rows))
}

func TestReaderBadJson(t *testing.T) {
	testJSON := ` {
   "rows": [
   {
   "id": 0,
   "first name": "tim",
   "last name": "sehn"
   bad
 },
 {
   "id": 1,
   "first name": "aaron",
   "last name": "son",
 },
 {
   "id": 2,
   "first name": "brian",
   "last name": "hendricks",
 }
 }
]
}`

	fs := filesys.EmptyInMemFS("/")
	require.NoError(t, fs.WriteFile("file.json", []byte(testJSON)))

	sch := sql.Schema{
		&sql.Column{
			Name:       "id",
			Type:       sql.Int64,
			PrimaryKey: true,
		},
		&sql.Column{
			Name:       "first name",
			Type:       sql.Text,
			PrimaryKey: false,
		},
		&sql.Column{
			Name:       "last name",
			Type:       sql.Text,
			PrimaryKey: false,
		},
	}

	reader, err := OpenJSONReader("file.json", fs, sql.NewPrimaryKeySchema(sch))
	require.NoError(t, err)

	err = nil
	for {
		_, err = reader.ReadSqlRow(context.Background())
		if err != nil {
			break
		}
	}
	assert.NotEqual(t, io.EOF, err)
	assert.Error(t, err)
}
