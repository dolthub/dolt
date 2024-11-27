// Copyright 2021 Dolthub, Inc.
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

package integration_test

import (
	"context"
	"encoding/base32"
	json2 "encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	"github.com/dolthub/dolt/go/store/types"
)

type jsonValueTest struct {
	name  string
	setup []testCommand
	query string
	rows  []sql.UntypedSqlRow
}

func TestJsonValues(t *testing.T) {
	if types.Format_Default != types.Format_LD_1 {
		t.Skip() // todo: convert to enginetests
	}

	SkipByDefaultInCI(t)
	setupCommon := []testCommand{
		{cmd.SqlCmd{}, args{"-q", `create table js (pk int primary key, js json);`}},
	}

	tests := []jsonValueTest{
		{
			name:  "create JSON table",
			setup: []testCommand{},
			query: "select * from js",
			rows:  []sql.UntypedSqlRow{},
		},
		{
			name: "insert into a JSON table",
			setup: []testCommand{
				{cmd.SqlCmd{}, args{"-q", `insert into js values (1, '{"a":1}'), (2, '{"b":2}');`}},
			},
			query: "select * from js",
			rows: []sql.UntypedSqlRow{
				{int32(1), json.MustNomsJSON(`{"a":1}`)},
				{int32(2), json.MustNomsJSON(`{"b":2}`)},
			},
		},
		{
			name: "update a JSON table",
			setup: []testCommand{
				{cmd.SqlCmd{}, args{"-q", `insert into js values (1, '{"a":1}'), (2, '{"b":2}');`}},
				{cmd.SqlCmd{}, args{"-q", `update js set js = '{"c":3}' where pk = 2;`}},
			},
			query: "select * from js",
			rows: []sql.UntypedSqlRow{
				{int32(1), json.MustNomsJSON(`{"a":1}`)},
				{int32(2), json.MustNomsJSON(`{"c":3}`)},
			},
		},
		{
			name: "delete from a JSON table",
			setup: []testCommand{
				{cmd.SqlCmd{}, args{"-q", `insert into js values (1, '{"a":1}'), (2, '{"b":2}');`}},
				{cmd.SqlCmd{}, args{"-q", `delete from js where pk = 2;`}},
			},
			query: "select * from js",
			rows: []sql.UntypedSqlRow{
				{int32(1), json.MustNomsJSON(`{"a":1}`)},
			},
		},
		{
			name: "merge a JSON table",
			setup: []testCommand{
				{cmd.SqlCmd{}, args{"-q", `insert into js values (1, '{"a":1}'), (2, '{"b":2}');`}},
				{cmd.AddCmd{}, args{"."}},
				{cmd.CommitCmd{}, args{"-m", "added a JSON table"}},
				{cmd.CheckoutCmd{}, args{"-b", "other"}},
				{cmd.SqlCmd{}, args{"-q", `update js set js = '{"b":22}' where pk = 2;`}},
				{cmd.CommitCmd{}, args{"-am", "update row pk = 2"}},
				{cmd.CheckoutCmd{}, args{env.DefaultInitBranch}},
				{cmd.SqlCmd{}, args{"-q", `update js set js = '{"a":11}' where pk = 1;`}},
				{cmd.CommitCmd{}, args{"-am", "update row pk = 1"}},
				{cmd.MergeCmd{}, args{"other"}},
			},
			query: "select * from js",
			rows: []sql.UntypedSqlRow{
				{int32(1), json.MustNomsJSON(`{"a":11}`)},
				{int32(2), json.MustNomsJSON(`{"b":22}`)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testJsonValue(t, test, setupCommon)
		})
	}
}

func testJsonValue(t *testing.T, test jsonValueTest, setupCommon []testCommand) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	cliCtx, verr := cmd.NewArgFreeCliContext(ctx, dEnv)
	require.NoError(t, verr)

	setup := append(setupCommon, test.setup...)
	for _, c := range setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	actRows, err := sqle.ExecuteSelect(dEnv, root, test.query)
	require.NoError(t, err)

	require.Equal(t, len(test.rows), len(actRows))
	for i := range test.rows {
		assert.Equal(t, len(test.rows[i]), actRows[i].Len())
		for j := range test.rows[i] {
			exp, act := test.rows[i][j], actRows[i].GetValue(j)

			// special logic for comparing JSONValues
			if js, ok := exp.(json.NomsJSON); ok {
				cmp, err := gmstypes.CompareJSON(js, act.(json.NomsJSON))
				require.NoError(t, err)
				assert.Zero(t, cmp)
			} else {
				assert.Equal(t, exp, act)
			}
		}
	}
}

// round-trips large random JSON objects through the SQL engine
func TestLargeJsonObjects(t *testing.T) {
	if types.Format_Default != types.Format_LD_1 {
		t.Skip() // todo: convert to enginetests
	}

	SkipByDefaultInCI(t)
	setupCommon := []testCommand{
		{cmd.SqlCmd{}, args{"-q", `create table js (pk int primary key, js json);`}},
	}

	// Generate large random JSON values
	const iterations = 25 // number of test iteration
	const numRows = 100   // number of rows/objects per test
	const objSize = 100   // number of key:value pairs per row/object
	const strLen = 10     // size of key, value strings

	k := 0
	for k < iterations {

		// generate a random byte string to slice into
		// JSON object key:value pairs
		bb := make([]byte, numRows*objSize*strLen*2)
		rand.Read(bb)
		// encode as base32 (longer than we need)
		ss := base32.StdEncoding.EncodeToString(bb)

		i := 0
		vals := make([]string, numRows)
		for i < numRows {

			// for each row, generate a big random object
			obj := make(map[string]interface{}, objSize)

			j := 0
			for j < objSize {
				start := strLen * 2 * i * j
				k := ss[start : start+strLen]
				v := ss[start+strLen : start+(strLen*2)]
				obj[k] = v
				j++
			}

			// marshal the random object into a string
			js, err := json2.Marshal(obj)
			require.NoError(t, err)
			vals[i] = string(js)

			i++
		}

		// for each row/object:
		// - create an expected output rows
		// - add an insert tuple to the setup query
		expected := make([]sql.UntypedSqlRow, numRows)

		query := strings.Builder{}
		query.WriteString("insert into js values (")
		seenOne := false

		for i, val := range vals {
			expected[i] = sql.NewUntypedRow(int32(i), json.MustNomsJSON(val)).(sql.UntypedSqlRow)

			if seenOne {
				query.WriteString("'),(")
			}
			seenOne = true
			query.WriteString(strconv.Itoa(i))
			query.WriteString(",'")
			query.WriteString(val)
		}
		query.WriteString("');")

		test := jsonValueTest{
			name: fmt.Sprintf("test large json objects (%d)", k),
			setup: []testCommand{
				{cmd.SqlCmd{}, args{"-q", query.String()}},
			},
			query: "select * from js;",
			rows:  expected,
		}

		t.Run(test.name, func(t *testing.T) {
			testJsonValue(t, test, setupCommon)
		})
		k++
	}
}
