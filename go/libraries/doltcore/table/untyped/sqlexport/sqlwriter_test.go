// Copyright 2019 Liquidata, Inc.
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

package sqlexport

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type StringBuilderCloser struct {
	strings.Builder
}

func (*StringBuilderCloser) Close() error {
	return nil
}

type test struct {
	name           string
	rows           []row.Row
	sch            schema.Schema
	expectedOutput string
}


func TestWriteRow(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	tableName := "people"

	dropCreateStatement := "DROP TABLE IF EXISTS `people`;\n" + sql.SchemaAsCreateStmt(tableName, dtestutils.TypedSchema)

	//type test struct {
	//	name           string
	//	rows           []row.Row
	//	sch            schema.Schema
	//	expectedOutput string
	//}

	tests := []test{
		{
			name: "simple row",
			rows: rs(dtestutils.NewTypedRow(id, "some guy", 100, false, strPointer("normie"))),
			sch:  dtestutils.TypedSchema,
			expectedOutput: dropCreateStatement + "\n" + "INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ("00000000-0000-0000-0000-000000000000","some guy",100,FALSE,"normie");` +
				"\n",
		},
		{
			name: "embedded quotes",
			rows: rs(dtestutils.NewTypedRow(id, `It's "Mister Perfect" to you`, 100, false, strPointer("normie"))),
			sch:  dtestutils.TypedSchema,
			expectedOutput: dropCreateStatement + "\n" + "INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ("00000000-0000-0000-0000-000000000000","It's \"Mister Perfect\" to you",100,FALSE,"normie");` +
				"\n",
		},
		{
			name: "two rows",
			rows: rs(
				dtestutils.NewTypedRow(id, "some guy", 100, false, strPointer("normie")),
				dtestutils.NewTypedRow(id, "guy personson", 0, true, strPointer("officially a person"))),
			sch: dtestutils.TypedSchema,
			expectedOutput: dropCreateStatement + "\n" +
				"INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ("00000000-0000-0000-0000-000000000000","some guy",100,FALSE,"normie");` + "\n" +
				"INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ("00000000-0000-0000-0000-000000000000","guy personson",0,TRUE,"officially a person");` + "\n",
		},
		{
			name: "null values",
			rows: rs(dtestutils.NewTypedRow(id, "some guy", 100, false, nil)),
			sch:  dtestutils.TypedSchema,
			expectedOutput: dropCreateStatement + "\n" + "INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ("00000000-0000-0000-0000-000000000000","some guy",100,FALSE,NULL);` +
				"\n",
		},
	}

	trickySch := dtestutils.CreateSchema(
		schema.NewColumn("a name with spaces", 0, types.FloatKind, false),
		schema.NewColumn("anotherColumn", 1, types.IntKind, true),
	)
	dropCreateTricky := "DROP TABLE IF EXISTS `people`;\n" + sql.SchemaAsCreateStmt(tableName, trickySch)

	tests = append(tests, test{
		name: "negative values and columns with spaces",
		rows: rs(dtestutils.NewRow(trickySch, types.Float(-3.14), types.Int(-42))),
		sch:  trickySch,
		expectedOutput: dropCreateTricky + "\n" + "INSERT INTO `people` (`a name with spaces`,`anotherColumn`) " +
			`VALUES (-3.14,-42);` +
			"\n",
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stringWr StringBuilderCloser
			w := &SqlExportWriter{
				tableName: tableName,
				sch:       tt.sch,
				wr:        &stringWr,
			}

			for _, r := range tt.rows {
				assert.NoError(t, w.WriteRow(context.Background(), r))
			}
			assert.Equal(t, tt.expectedOutput, stringWr.String())
		})
	}
}

func TestDeleteRow(t *testing.T) {
	tableName := "tricky"
	trickySch := dtestutils.CreateSchema(
		schema.NewColumn("anotherCol", 0, types.FloatKind, false),
		schema.NewColumn("a name with spaces", 1, types.IntKind, true),
	)

	tests := []test{
		{
			name:
			"negative values and columns with spaces",
			rows: rs(dtestutils.NewRow(trickySch, types.Float(-3.14), types.Int(-42))),
			sch:  trickySch,
			expectedOutput: "DELETE FROM `tricky` WHERE (`a name with spaces`=`-42`);" + "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stringWr StringBuilderCloser
			w := &SqlExportWriter{
				tableName: tableName,
				sch:       tt.sch,
				wr:        &stringWr,
			}

			for _, r := range tt.rows {
				assert.NoError(t, w.WriteDeleteRow(context.Background(), r))
			}
			assert.Equal(t, tt.expectedOutput, stringWr.String())
		})
	}
}

func TestUpdateRow(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	tableName := "people"

	tests := []test{
		{
			name: "simple row",
			rows: rs(dtestutils.NewTypedRow(id, "some guy", 100, false, strPointer("normie"))),
			sch:  dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`=`some guy`,`age`=`100`,`is_married`=`FALSE`,`title`=`normie` WHERE (`id`=`00000000-0000-0000-0000-000000000000`);" + "\n",
		},
		{
			name: "embedded quotes",
			rows: rs(dtestutils.NewTypedRow(id, `It's "Mister Perfect" to you`, 100, false, strPointer("normie"))),
			sch:  dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`=`It's \"Mister Perfect\" to you`,`age`=`100`,`is_married`=`FALSE`,`title`=`normie` WHERE (`id`=`00000000-0000-0000-0000-000000000000`);" + "\n",
		},
		{
			name: "two rows",
			rows: rs(
				dtestutils.NewTypedRow(id, "some guy", 100, false, strPointer("normie")),
				dtestutils.NewTypedRow(id, "guy personson", 0, true, strPointer("officially a person"))),
			sch: dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`=`some guy`,`age`=100,`is_married`=FALSE,`title`=`normie` WHERE (`id`=`00000000-0000-0000-0000-000000000000`);" + "\n" +
				"UPDATE `people` SET `name`=`guy personson`,`age`=0,`is_married`=TRUE,`title`=`officially a person` WHERE (`id`=`00000000-0000-0000-0000-000000000000`);" + "\n",
		},
		{
			name: "null values",
			rows: rs(dtestutils.NewTypedRow(id, "some guy", 100, false, nil)),
			sch:  dtestutils.TypedSchema,
			expectedOutput: "UPDATE `people` SET `name`=`some guy`,`age`=`100`,`is_married`=`FALSE`,`title`=`NULL` WHERE (`id`=`00000000-0000-0000-0000-000000000000`);" + "\n",
		},
	}

	trickySch := dtestutils.CreateSchema(
		schema.NewColumn("a name with spaces", 0, types.FloatKind, false),
		schema.NewColumn("anotherColumn", 1, types.IntKind, true),
	)

	tests = append(tests, test{
		name: "negative values and columns with spaces",
		rows: rs(dtestutils.NewRow(trickySch, types.Float(-3.14), types.Int(-42))),
		sch:  trickySch,
		expectedOutput: "UPDATE `people` SET `a name with spaces`=-3.14 WHERE (`anotherColumn`=`-42`);" + "\n",
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stringWr StringBuilderCloser
			w := &SqlExportWriter{
				tableName: tableName,
				sch:       tt.sch,
				wr:        &stringWr,
			}

			for _, r := range tt.rows {
				assert.NoError(t, w.WriteUpdateRow(context.Background(), r))
			}
			assert.Equal(t, tt.expectedOutput, stringWr.String())
		})
	}
}

func TestEndToEnd(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	tableName := "people"

	dropCreateStatement := "DROP TABLE IF EXISTS `people`;\n" + sql.SchemaAsCreateStmt(tableName, dtestutils.TypedSchema)

	type test struct {
		name           string
		rows           []row.Row
		sch            schema.Schema
		expectedOutput string
	}

	tests := []test{
		{
			name: "two rows",
			rows: rs(
				dtestutils.NewTypedRow(id, "some guy", 100, false, strPointer("normie")),
				dtestutils.NewTypedRow(id, "guy personson", 0, true, strPointer("officially a person"))),
			sch: dtestutils.TypedSchema,
			expectedOutput: dropCreateStatement + "\n" +
				"INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ("00000000-0000-0000-0000-000000000000","some guy",100,FALSE,"normie");` + "\n" +
				"INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ("00000000-0000-0000-0000-000000000000","guy personson",0,TRUE,"officially a person");` + "\n",
		},
		{
			name:           "no rows",
			sch:            dtestutils.TypedSchema,
			expectedOutput: dropCreateStatement + "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stringWr StringBuilderCloser
			w := &SqlExportWriter{
				tableName: tableName,
				sch:       tt.sch,
				wr:        &stringWr,
			}

			for _, r := range tt.rows {
				assert.NoError(t, w.WriteRow(context.Background(), r))
			}

			assert.NoError(t, w.Close(context.Background()))
			assert.Equal(t, tt.expectedOutput, stringWr.String())
		})
	}
}

func rs(rs ...row.Row) []row.Row {
	return rs
}

func strPointer(s string) *string {
	return &s
}
