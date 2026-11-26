// Copyright 2026 Dolthub, Inc.
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

package commands

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

// TestDumpTriggerWithCompoundBody checks that a trigger whose body is a BEGIN ... END block with several statements
// is written to the dump in a form that can be loaded again.
func TestDumpTriggerWithCompoundBody(t *testing.T) {
	ctx := t.Context()
	dEnv := DEnvWithSeedDataForTest(ctx, t)
	cliCtx := NewArgFreeCliContextForTest(ctx, t, dEnv)

	createTrigger := "delimiter //\n" +
		"CREATE TRIGGER people_bi BEFORE INSERT ON people FOR EACH ROW\n" +
		"BEGIN\n" +
		"  SET NEW.title = 'Trainee';\n" +
		"  SET NEW.age = 18;\n" +
		"END//\n"
	require.Equal(t, 0, SqlCmd{}.Exec(ctx, "dolt sql", []string{"-q", createTrigger}, dEnv, cliCtx))
	require.Equal(t, 0, DumpCmd{}.Exec(ctx, "dolt dump", []string{"--no-create-db"}, dEnv, cliCtx))

	dump, err := dEnv.FS.ReadFile("doltdump.sql")
	require.NoError(t, err)

	newEnv := DEnvWithSeedDataForTest(ctx, t)
	newCliCtx := NewArgFreeCliContextForTest(ctx, t, newEnv)
	require.Equal(t, 0, SqlCmd{}.Exec(ctx, "dolt sql", []string{"-q", string(dump)}, newEnv, newCliCtx), "loading the dump failed:\n%s", dump)

	insert := "INSERT INTO people (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'Jane Janeson');"
	require.Equal(t, 0, SqlCmd{}.Exec(ctx, "dolt sql", []string{"-q", insert}, newEnv, newCliCtx))

	root, err := newEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	rows, err := sqle.ExecuteSelect(ctx, newEnv, root, "SELECT title, age FROM people WHERE id = '00000000-0000-0000-0000-000000000003'")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "Trainee", rows[0][0])
	require.EqualValues(t, 18, rows[0][1])
}
