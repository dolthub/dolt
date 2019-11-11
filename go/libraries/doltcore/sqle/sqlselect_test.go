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

package sqle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
)

// Set to the name of a single test to run just that test, useful for debugging
const singleSelectQueryTest = "" //"Natural join with join clause"

// Set to false to run tests known to be broken
const skipBrokenSelect = true

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

type testCommitClock struct {
	unixNano int64
}

func (tcc *testCommitClock) Now() time.Time {
	now := time.Unix(0, tcc.unixNano)
	tcc.unixNano += int64(time.Millisecond)
	return now
}

// Tests the given query on a freshly created dataset, asserting that the result has the given schema and rows. If
// expectedErr is set, asserts instead that the execution returns an error that matches.
func testSelectQuery(t *testing.T, test SelectTest) {
	if (test.ExpectedRows == nil) != (test.ExpectedSchema == nil) {
		require.Fail(t, "Incorrect test setup: schema and rows must both be provided if one is")
	}

	if len(singleSelectQueryTest) > 0 && test.Name != singleSelectQueryTest {
		t.Skip("Skipping tests until " + singleSelectQueryTest)
	}

	if len(singleSelectQueryTest) == 0 && test.SkipOnSqlEngine && skipBrokenSelect {
		t.Skip("Skipping test broken on SQL engine")
	}

	tcc := &testCommitClock{}
	doltdb.CommitNowFunc = tcc.Now
	doltdb.CommitLoc = time.UTC

	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	if test.AdditionalSetup != nil {
		test.AdditionalSetup(t, dEnv)
	}

	root, _ := dEnv.WorkingRoot(context.Background())
	actualRows, sch, err := executeSelect(context.Background(), dEnv, test.ExpectedSchema, root, test.Query)
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
