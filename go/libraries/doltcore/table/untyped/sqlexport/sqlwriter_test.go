package sqlexport

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

type StringBuilderCloser struct {
	strings.Builder
}
func (*StringBuilderCloser) Close() error {
	return nil
}

func TestWriteRow(t *testing.T) {
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
			name: "no rows",
			sch: dtestutils.TypedSchema,
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
