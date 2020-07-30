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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/sqlfmt"
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

func TestEndToEnd(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	tableName := "people"

	dropCreateStatement := sqlfmt.DropTableIfExistsStmt(tableName) + "\n" + sqlfmt.CreateTableStmtWithTags(tableName, dtestutils.TypedSchema, nil, nil)

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
				`VALUES ('00000000-0000-0000-0000-000000000000','some guy',100,FALSE,'normie');` + "\n" +
				"INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ('00000000-0000-0000-0000-000000000000','guy personson',0,TRUE,'officially a person');` + "\n",
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
