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

package enginetest

import (
	"fmt"
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

// DoltPreparedScripts tests dolt-specific prepared statements through
// the handler interface.
func DoltPreparedScripts(t *testing.T, harness enginetest.Harness) {
	tests := []queries.QueryTest{
		{
			Query:    "set @@SESSION.dolt_show_system_tables = 1",
			Expected: []sql.Row{{types.NewOkResult(0)}},
		},
		{
			Query: "SELECT table_name FROM information_schema.columns WHERE table_name = ? group by table_name ORDER BY ORDINAL_POSITION",
			Bindings: map[string]sqlparser.Expr{
				"v1": sqlparser.NewStrVal([]byte("dolt_history_mytable")),
			},
			Expected: []sql.Row{{"dolt_history_mytable"}},
		},
	}

	harness.Setup(setup.MydbData, setup.MytableData)
	e, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer e.Close()

	enginetest.RunQueryWithContext(t, e, harness, nil, "CREATE TABLE a (x int, y int, z int)")
	enginetest.RunQueryWithContext(t, e, harness, nil, "INSERT INTO a VALUES (0,1,1), (1,1,1), (2,1,1), (3,2,2), (4,2,2)")
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s", tt.Query), func(t *testing.T) {
			ctx := enginetest.NewContext(harness)
			_, err := e.PrepareQuery(ctx, tt.Query)
			require.NoError(t, err)
			enginetest.TestQueryWithContext(t, ctx, e, harness, tt.Query, tt.Expected, tt.ExpectedColumns, tt.Bindings, nil)
		})
	}
}
