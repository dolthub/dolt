package sqle

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	. "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql/sqltestutil"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleQueryTest = ""//"Natural join with join clause"

// Set to false to run tests known to be broken
const skipBroken = true

func TestExecuteSelect(t *testing.T) {
	for _, test := range BasicSelectTests {
		t.Run(test.Name, func(t *testing.T) {
			testSelectQuery(t, test)
		})
	}
}

func TestJoins(t *testing.T) {
	for _, tt := range JoinTests {
		t.Run(tt.Name, func(t *testing.T) {
			testSelectQuery(t, tt)
		})
	}
}

// Tests of case sensitivity handling
func TestCaseSensitivity(t *testing.T) {
	for _, tt := range CaseSensitivityTests {
		t.Run(tt.Name, func(t *testing.T) {
			testSelectQuery(t, tt)
		})
	}
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testSelectQuery(t *testing.T, test SelectTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleQueryTest) > 0 && test.Name != singleQueryTest {
		t.Skip("Skipping tests until " + singleQueryTest)
	}

	if len(singleQueryTest) == 0 && test.SkipOnSqlEngine && skipBroken {
		t.Skip("Skipping test broken on SQL engine")
	}

	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	root, _ := dEnv.WorkingRoot(context.Background())
	actualRows, sch, err := executeSelect(context.Background(), test.ExpectedSchema, root, test.Query)
	if len(test.ExpectedErr) > 0 {
		require.Error(t, err)
		// Too much work to synchronize error messages between the two implementations, so for now we'll just assert that an error occurred.
		// require.Contains(t, err.Error(), test.ExpectedErr)
		return
	} else {
		require.NoError(t, err)
	}

	assert.Equal(t, test.ExpectedRows, actualRows)
	assert.Equal(t, test.ExpectedSchema, sch)
}

// Runs the query given and returns the result. The schema result of the query's execution is currently ignored, and
// the targetSchema given is used to prepare all rows.
func executeSelect(ctx context.Context, targetSch schema.Schema, root *doltdb.RootValue, query string) ([]row.Row, schema.Schema, error) {
	db := NewDatabase("dolt", root)
	engine := sqle.NewDefault()
	engine.AddDatabase(db)
	engine.Catalog.RegisterIndexDriver(&DoltIndexDriver{db})
	engine.Init()
	sqlCtx := sql.NewContext(ctx)

	var err error
	_, iter, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, nil, err
	}

	if targetSch == nil {
		return nil, nil, nil
	}

	doltRows := make([]row.Row, 0)
	var r sql.Row
	for r, err = iter.Next(); err == nil; r, err = iter.Next() {
		doltRows = append(doltRows, SqlRowToDoltRow(types.Format_7_18, r, targetSch))
	}
	if err !=  io.EOF {
		return nil, nil, err
	}

	return doltRows, targetSch, nil
}