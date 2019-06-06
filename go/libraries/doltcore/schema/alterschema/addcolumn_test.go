package alterschema

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
)

const tableName = "people"

func TestAddColumnToSchema(t *testing.T) {
	tests := []struct {
		name           string
		tag            uint64
		newColName     string
		colKind        types.NomsKind
		nullable       Nullable
		defaultVal     *types.Value
		expectedSchema schema.Schema
		expectedErr    string
	}{
		{
			name:       "add string column",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.StringKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schema.NewColumn("newCol", dtestutils.NextTag, types.StringKind, false)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := createEnvWithSeedData(t)
			ctx := context.Background()

			root, err := dEnv.WorkingRoot(ctx)
			assert.NoError(t, err)
			tbl, _ := root.GetTable(ctx, tableName)

			updatedTable, err := AddColumnToSchema(ctx, dEnv, tbl, tt.tag, tt.newColName, tt.colKind, tt.nullable, tt.defaultVal)
			if len(tt.expectedErr) > 0 {
			  assert.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectedSchema, updatedTable.GetSchema(ctx))
		})
	}
}

func createEnvWithSeedData(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	imt, sch := dtestutils.CreateTestDataTable(true)

	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(context.Background(), dEnv.DoltDB.ValueReadWriter(), sch)

	_, _, err := table.PipeRows(context.Background(), rd, wr, false)
	rd.Close(context.Background())
	wr.Close(context.Background())

	if err != nil {
		t.Error("Failed to seed initial data", err)
	}

	err = dEnv.PutTableToWorking(context.Background(), *wr.GetMap(), wr.GetSchema(), tableName)

	if err != nil {
		t.Error("Unable to put initial value of table in in mem noms db", err)
	}

	return dEnv
}
