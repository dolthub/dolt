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

package sqlexport

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/store/types"
)

type StringBuilderCloser struct {
	strings.Builder
}

func (*StringBuilderCloser) Close() error {
	return nil
}

func TestEndToEnd(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	tableName := "people"

	dropCreateStatement := sqlfmt.DropTableIfExistsStmt(tableName) + "\n" +
		"CREATE TABLE `people` (\n" +
		"  `id` varchar(16383) NOT NULL,\n" +
		"  `name` varchar(16383) NOT NULL,\n" +
		"  `age` bigint unsigned NOT NULL,\n" +
		"  `is_married` bigint NOT NULL,\n" +
		"  `title` varchar(16383),\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `idx_name` (`name`),\n" +
		"  CONSTRAINT `test-check` CHECK ((`age` < 123))\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"

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
				`VALUES ('00000000-0000-0000-0000-000000000000','some guy',100,0,'normie');` + "\n" +
				"INSERT INTO `people` (`id`,`name`,`age`,`is_married`,`title`) " +
				`VALUES ('00000000-0000-0000-0000-000000000000','guy personson',0,1,'officially a person');` + "\n",
		},
		{
			name:           "no rows",
			sch:            dtestutils.TypedSchema,
			expectedOutput: dropCreateStatement + "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtestutils.CreateTestEnv()
			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			empty, err := types.NewMap(ctx, root.VRW())
			require.NoError(t, err)

			indexes := durable.NewIndexSet(ctx, root.VRW())
			indexes, err = indexes.PutNomsIndex(ctx, dtestutils.IndexName, empty)
			require.NoError(t, err)

			tbl, err := doltdb.NewNomsTable(ctx, root.VRW(), tt.sch, empty, indexes, nil)
			require.NoError(t, err)
			root, err = root.PutTable(ctx, tableName, tbl)
			require.NoError(t, err)

			var stringWr StringBuilderCloser
			w := &SqlExportWriter{
				tableName: tableName,
				sch:       tt.sch,
				wr:        &stringWr,
				root:      root,
			}

			for _, r := range tt.rows {
				assert.NoError(t, w.WriteRow(ctx, r))
			}

			assert.NoError(t, w.Close(ctx))
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
