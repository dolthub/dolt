package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	. "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
)

func TestExecuteSelect(t *testing.T) {
	for _, test := range BasicSelectTests {
		t.Run(test.Name, func(t *testing.T) {
			runSelectTest(t, test)
		})
	}
}

func TestJoins(t *testing.T) {
	for _, test := range JoinTests {
		t.Run(test.Name, func(t *testing.T) {
			runSelectTest(t, test)
		})
	}
}

func TestCaseSensitivity(t *testing.T) {
	for _, test := range CaseSensitivityTests {
		t.Run(test.Name, func(t *testing.T) {
			runSelectTest(t, test)
		})
	}
}

func TestBuildSelectQueryPipeline(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		expectedSchema  schema.Schema
		expectedNumRows int
	}{
		{
			name:            "Test select *",
			query:           "select * from people",
			expectedNumRows: len([]row.Row{Homer, Marge, Bart, Lisa, Moe, Barney}),
			expectedSchema:  CompressSchema(PeopleTestSchema),
		},
		{
			name:            "Test select columns",
			query:           "select age, id from people",
			expectedNumRows: len([]row.Row{Homer, Marge, Bart, Lisa, Moe, Barney}),
			expectedSchema:  CompressSchema(PeopleTestSchema, "age", "id"),
		},
	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		CreateTestDatabase(dEnv, t)
		root, _ := dEnv.WorkingRoot(context.Background())

		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.Select)

		t.Run(tt.name, func(t *testing.T) {
			p, statement, _ := BuildSelectQueryPipeline(context.Background(), root, s)
			var outputRows int
			p.SetOutput(pipeline.ProcFuncForSinkFunc(
				func(r row.Row, props pipeline.ReadableMap) error {
					outputRows++
					return nil
				}))
			p.SetBadRowCallback(func(*pipeline.TransformRowFailure) (quit bool) {
				return true
			})
			p.Start()
			p.Wait()

			assert.Equal(t, tt.expectedNumRows, outputRows)
			assert.Equal(t, tt.expectedSchema, statement.ResultSetSchema)
		})
	}
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func runSelectTest(t *testing.T, test SelectTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	root, _ := dEnv.WorkingRoot(context.Background())
	sqlStatement, _ := sqlparser.Parse(test.Query)
	s := sqlStatement.(*sqlparser.Select)

	actualRows, sch, err := ExecuteSelect(context.Background(), root, s)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		require.Contains(t, err.Error(), test.ExpectedErr)
		return
	} else {
		require.NoError(t, err)
	}

	assert.Equal(t, test.ExpectedRows, actualRows)
	assert.Equal(t, test.ExpectedSchema, sch)
}
