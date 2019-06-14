package sqlexport

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
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

	tests := []struct {
		name string
		tableName string
		row row.Row
		sch schema.Schema
		expectedStatement string
	}{
		{
			name:      "simple row",
			tableName: "people",
			row:       dtestutils.NewTypedRow(id, "some guy", 100, false, strPointer("normie")),
			sch:       dtestutils.TypedSchema,
			expectedStatement: "INSERT INTO people (`id`,`name`,`age`,`is_married`,`title`) " +
					`VALUES ("00000000-0000-0000-0000-000000000000","some guy",100,FALSE,"normie");` +
					"\n",
		},
		{
			name:      "null values",
			tableName: "people",
			row:       dtestutils.NewTypedRow(id, "some guy", 100, false, nil),
			sch:       dtestutils.TypedSchema,
			expectedStatement: "INSERT INTO people (`id`,`name`,`age`,`is_married`,`title`) " +
					`VALUES ("00000000-0000-0000-0000-000000000000","some guy",100,FALSE,NULL);` +
					"\n",
		},
		{
			name:      "negative values and columns with spaces",
			tableName: "people",
			row: dtestutils.NewRow(dtestutils.CreateSchema(
				schema.NewColumn("a name with spaces", 0, types.FloatKind, false),
				schema.NewColumn("anotherColumn", 1, types.IntKind, true),
			), types.Float(-3.14), types.Int(-42)),
			sch: dtestutils.CreateSchema(
				schema.NewColumn("a name with spaces", 0, types.FloatKind, false),
				schema.NewColumn("anotherColumn", 1, types.IntKind, true),
			),
			expectedStatement: "INSERT INTO people (`a name with spaces`,`anotherColumn`) " +
					`VALUES (-3.14,-42);` +
					"\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stringWr StringBuilderCloser
			w := &SqlExportWriter{
				tableName: tt.tableName,
				sch:       tt.sch,
				wr:        &stringWr,
			}

			assert.NoError(t, w.WriteRow(context.Background(), tt.row))
			assert.Equal(t, tt.expectedStatement, stringWr.String())
		})
	}
}

func strPointer(s string) *string {
	return &s
}
