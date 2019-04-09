package sql

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/xwb1989/sqlparser"
)

func TestExecuteInsert(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "insert one row, all columns",
			query: `insert into people (id, first, last, is_married, age, rating) values 
					(7, "Maggie", "Simpson", false, 1, 5.1)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			createTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot()

			sqlStatement, _ := sqlparser.Parse(tt.query)
			s := sqlStatement.(*sqlparser.Insert)

			result, err := ExecuteInsert(dEnv.DoltDB, root, s, tt.query)
			assert.NotNil(t, result)
			assert.Nil(t, err)
		})
	}
}
