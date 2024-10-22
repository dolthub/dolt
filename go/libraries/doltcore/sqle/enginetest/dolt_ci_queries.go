// Copyright 2024 Dolthub, Inc.
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
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

var DoltCIConfigTests = []queries.ScriptTest{
	{
		Name: "select * from dolt_ci_workflows;",
		SetUpScript: []string{
			"insert into dolt_ci_workflows values" +
				"('workflow_1', TIMESTAMP('2024-08-15 00:00:00'), TIMESTAMP('2024-08-15 00:00:00'))," +
				" ('workflow_2', TIMESTAMP('2024-08-15 00:00:00'), TIMESTAMP('2024-08-15 00:00:00'))," +
				" ('workflow_3', TIMESTAMP('2024-08-15 00:00:00'), TIMESTAMP('2024-08-15 00:00:00'))",
			"call dolt_commit('-Am', 'create three workflows');",
			"insert into dolt_ci_workflow_events values" +
				"('uuid_1', 'workflow_1', 0)," +
				" ('uuid_2', 'workflow_2', 1)," +
				" ('uuid_3', 'workflow_3', 2)",
			"call dolt_commit('-Am', 'create three workflow events');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_ci_workflows;",
				Expected: []sql.Row{
					sql.Row{"workflow_1", time.Date(2024, time.August, 15, 0, 0, 0, 0, time.UTC), time.Date(2024, time.August, 15, 0, 0, 0, 0, time.UTC)},
					sql.Row{"workflow_2", time.Date(2024, time.August, 15, 0, 0, 0, 0, time.UTC), time.Date(2024, time.August, 15, 0, 0, 0, 0, time.UTC)},
					sql.Row{"workflow_3", time.Date(2024, time.August, 15, 0, 0, 0, 0, time.UTC), time.Date(2024, time.August, 15, 0, 0, 0, 0, time.UTC)},
				},
			},
			{
				Query: "select * from dolt_ci_workflow_events;",
				Expected: []sql.Row{
					sql.Row{"uuid_1", "workflow_1", 0},
					sql.Row{"uuid_2", "workflow_2", 1},
					sql.Row{"uuid_3", "workflow_3", 2},
				},
			},
		},
	},
}

func RunDoltCIConfigTests(t *testing.T, h DoltEnginetestHarness) {
	for _, script := range DoltCIConfigTests {
		func() {
			h := h.NewHarness(t)
			defer h.Close()
			enginetest.TestScriptPrepared(t, h, script)
		}()
	}
}
