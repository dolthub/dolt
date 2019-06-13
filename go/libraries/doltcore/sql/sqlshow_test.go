package sql

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/xwb1989/sqlparser"
)

func TestExecuteShow(t *testing.T) {
	peopleSchemaStr, _ := SchemaAsCreateStmt("people", peopleTestSchema)

	peopleSchemaRows := rs(
		newResultSetRow(types.String("id"), types.String("int"), types.String("NO"), types.String("PRI"), types.String("NULL"), types.String("")),
		newResultSetRow(types.String("first"), types.String("varchar"), types.String("NO"), types.String(""), types.String("NULL"), types.String("")),
		newResultSetRow(types.String("last"), types.String("varchar"), types.String("NO"), types.String(""), types.String("NULL"), types.String("")),
		newResultSetRow(types.String("is_married"), types.String("bool"), types.String("YES"), types.String(""), types.String("NULL"), types.String("")),
		newResultSetRow(types.String("age"), types.String("int"), types.String("YES"), types.String(""), types.String("NULL"), types.String("")),
		newResultSetRow(types.String("rating"), types.String("float"), types.String("YES"), types.String(""), types.String("NULL"), types.String("")),
		newResultSetRow(types.String("uuid"), types.String("uuid"), types.String("YES"), types.String(""), types.String("NULL"), types.String("")),
		newResultSetRow(types.String("num_episodes"), types.String("int unsigned"), types.String("YES"), types.String(""), types.String("NULL"), types.String("")),
	)

	tests := []struct {
		name           string
		query          string
		expectedRows   []row.Row
		expectedSchema schema.Schema
		expectedErr    string
	}{
		{
			name:  "show create table",
			query: "show create table people",
			expectedRows: rs(
				newResultSetRow(types.String("people"), types.String(peopleSchemaStr)),
			),
			expectedSchema: showCreateTableSchema(),
		},
		{
			name:  "show create table case insensitive",
			query: "show create table PeOPle",
			expectedRows: rs(
				newResultSetRow(types.String("people"), types.String(peopleSchemaStr)),
			),
			expectedSchema: showCreateTableSchema(),
		},
		{
			name:  "show tables",
			query: "show tables",
			expectedRows: rs(
				newResultSetRow(types.String("appearances")),
				newResultSetRow(types.String("episodes")),
				newResultSetRow(types.String("people")),
			),
			expectedSchema: showTablesSchema(),
		},
		{
			name:           "show columns from table",
			query:          "show columns from people",
			expectedRows:   peopleSchemaRows,
			expectedSchema: showColumnsSchema(),
		},
		{
			name:           "show columns from table case insensitive",
			query:          "show columns from PeOpLe",
			expectedRows:   peopleSchemaRows,
			expectedSchema: showColumnsSchema(),
		},
		{
			name:        "show columns from unknown table",
			query:       "show columns from notFound",
			expectedErr: "Unknown table: 'notFound'",
		},
		{
			name:           "describe table",
			query:          "describe people",
			expectedRows:   peopleSchemaRows,
			expectedSchema: showColumnsSchema(),
		},
		{
			name:           "describe table case insensitive",
			query:          "describe PeOpLE",
			expectedRows:   peopleSchemaRows,
			expectedSchema: showColumnsSchema(),
		},
		{
			name:        "describe unknown table",
			query:       "describe notFound",
			expectedErr: "Unknown table: 'notFound'",
		},
		{
			name:        "show databases",
			query:       "show databases",
			expectedErr: "Unsupported show statement",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			createTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot(context.Background())

			sqlStatement, err := sqlparser.Parse(tt.query)
			if err != nil {
				assert.FailNow(t, "Couldn't parse query "+tt.query, "%v", err.Error())
			}

			if tt.expectedRows != nil && tt.expectedSchema == nil {
				require.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
			}

			s := sqlStatement.(*sqlparser.Show)

			rows, sch, err := ExecuteShow(context.Background(), root, s)

			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectedRows, rows)
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
}
