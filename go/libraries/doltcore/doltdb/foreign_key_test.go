// Copyright 2020 Dolthub, Inc.
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

package doltdb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/store/types"
)

func TestForeignKeys(t *testing.T) {
	t.Skip("foreign key representation has changed, need to update tests")
	for _, test := range foreignKeyTests {
		t.Run(test.name, func(t *testing.T) {
			testForeignKeys(t, test)
		})
	}
}

func TestForeignKeyErrors(t *testing.T) {
	skipNewFormat(t)
	cmds := []testCommand{
		{commands.SqlCmd{}, []string{"-q", `CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));`}},
		{commands.SqlCmd{}, []string{"-q", `CREATE TABLE test2(pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1),` +
			`CONSTRAINT child_fk FOREIGN KEY (v1) REFERENCES test(v1));`}},
	}

	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()

	for _, c := range cmds {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}

	exitCode := commands.SqlCmd{}.Exec(ctx, commands.SqlCmd{}.Name(), []string{"-q", `ALTER TABLE test MODIFY v1 INT;`}, dEnv)
	require.Equal(t, 1, exitCode)
	exitCode = commands.SqlCmd{}.Exec(ctx, commands.SqlCmd{}.Name(), []string{"-q", `ALTER TABLE test2 MODIFY v1 INT;`}, dEnv)
	require.Equal(t, 1, exitCode)
}

func skipNewFormat(t *testing.T) {
	if types.IsFormat_DOLT_1(types.Format_Default) {
		t.Skip()
	}
}

type foreignKeyTest struct {
	name  string
	setup []testCommand
	fks   []doltdb.ForeignKey
}

type testCommand struct {
	cmd  cli.Command
	args []string
}

var fkSetupCommon = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table parent (" +
		"id int," +
		"v1 int," +
		"v2 int," +
		"index v1_idx (v1)," +
		"index v2_idx (v2)," +
		"primary key(id));"}},
	{commands.SqlCmd{}, []string{"-q", "create table child (" +
		"id int, " +
		"v1 int," +
		"v2 int," +
		"primary key(id));"}},
}

func testForeignKeys(t *testing.T, test foreignKeyTest) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()

	for _, c := range fkSetupCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}
	for _, c := range test.setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	fkc, err := root.GetForeignKeyCollection(ctx)
	require.NoError(t, err)

	assert.Equal(t, test.fks, fkc.AllKeys())

	for _, fk := range test.fks {
		// verify parent index
		pt, _, ok, err := root.GetTableInsensitive(ctx, fk.ReferencedTableName)
		require.NoError(t, err)
		require.True(t, ok)
		ps, err := pt.GetSchema(ctx)
		require.NoError(t, err)
		pi, ok := ps.Indexes().GetByNameCaseInsensitive(fk.ReferencedTableIndex)
		require.True(t, ok)
		require.Equal(t, fk.ReferencedTableColumns, pi.IndexedColumnTags())

		// verify child index
		ct, _, ok, err := root.GetTableInsensitive(ctx, fk.TableName)
		require.NoError(t, err)
		require.True(t, ok)
		cs, err := ct.GetSchema(ctx)
		require.NoError(t, err)
		ci, ok := cs.Indexes().GetByNameCaseInsensitive(fk.TableIndex)
		require.True(t, ok)
		require.Equal(t, fk.TableColumns, ci.IndexedColumnTags())
	}
}

var foreignKeyTests = []foreignKeyTest{
	{
		name: "create foreign key",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v1_idx (v1)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child add 
				constraint child_fk foreign key (v1) references parent(v1)`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "child_fk",
				TableName:              "child",
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
			},
		},
	},
	{
		name: "create multi-column foreign key",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `alter table parent add index v1v2_idx (v1, v2)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v1v2_idx (v1, v2)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child add 
				constraint multi_col foreign key (v1, v2) references parent(v1, v2)`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "multi_col",
				TableName:              "child",
				TableIndex:             "v1v2_idx",
				TableColumns:           []uint64{1215, 8734},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1v2_idx",
				ReferencedTableColumns: []uint64{6269, 7947},
			},
		},
	},
	{
		name: "create multiple foreign keys",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v1_idx (v1)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v2_idx (v2)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add constraint fk1 foreign key (v1) references parent(v1)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add constraint fk2 foreign key (v2) references parent(v2)`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "fk1",
				TableName:              "child",
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
			},
			{
				Name:                   "fk2",
				TableName:              "child",
				TableIndex:             "v2_idx",
				TableColumns:           []uint64{8734},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v2_idx",
				ReferencedTableColumns: []uint64{7947},
			},
		},
	},
	{
		name: "create table with foreign key",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `create table new_table (
				id int,
				v1 int,
				constraint new_fk foreign key (v1) references parent(v1),
				primary key(id));`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:      "new_fk",
				TableName: "new_table",
				// unnamed indexes take the column name
				TableIndex:             "v1",
				TableColumns:           []uint64{7597},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
			},
		},
	},
	{
		name: "create foreign keys with update or delete rules",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v1_idx (v1)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v2_idx (v2)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add constraint fk1 foreign key (v1) references parent(v1) on update cascade`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add constraint fk2 foreign key (v2) references parent(v2) on delete set null`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "fk1",
				TableName:              "child",
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_Cascade,
			},
			{
				Name:                   "fk2",
				TableName:              "child",
				TableIndex:             "v2_idx",
				TableColumns:           []uint64{8734},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v2_idx",
				ReferencedTableColumns: []uint64{7947},
				OnDelete:               doltdb.ForeignKeyReferentialAction_SetNull,
			},
		},
	},
	{
		name: "create single foreign key with update and delete rules",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v1_idx (v1)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add constraint child_fk foreign key (v1) references parent(v1) on update cascade on delete cascade`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "child_fk",
				TableName:              "child",
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_Cascade,
				OnDelete:               doltdb.ForeignKeyReferentialAction_Cascade,
			},
		},
	},
	{
		name: "create foreign keys with all update and delete rules",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", "alter table parent add column v3 int;"}},
			{commands.SqlCmd{}, []string{"-q", "alter table parent add column v4 int;"}},
			{commands.SqlCmd{}, []string{"-q", "alter table parent add column v5 int;"}},
			{commands.SqlCmd{}, []string{"-q", "alter table parent add index v3_idx (v3);"}},
			{commands.SqlCmd{}, []string{"-q", "alter table parent add index v4_idx (v4);"}},
			{commands.SqlCmd{}, []string{"-q", "alter table parent add index v5_idx (v5);"}},
			{commands.SqlCmd{}, []string{"-q", `create table sibling (
					id int,
					v1 int,
					v2 int,
					v3 int,
					v4 int,
					v5 int,
					constraint fk1 foreign key (v1) references parent(v1),
					constraint fk2 foreign key (v2) references parent(v2) on delete restrict on update restrict,
					constraint fk3 foreign key (v3) references parent(v3) on delete cascade on update cascade,
					constraint fk4 foreign key (v4) references parent(v4) on delete set null on update set null,
					constraint fk5 foreign key (v5) references parent(v5) on delete no action on update no action,
					primary key (id));`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "fk1",
				TableName:              "sibling",
				TableIndex:             "v1",
				TableColumns:           []uint64{16080},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
			},
			{
				Name:                   "fk2",
				TableName:              "sibling",
				TableIndex:             "v2",
				TableColumns:           []uint64{7576},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v2_idx",
				ReferencedTableColumns: []uint64{7947},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_Restrict,
				OnDelete:               doltdb.ForeignKeyReferentialAction_Restrict,
			},
			{
				Name:                   "fk3",
				TableName:              "sibling",
				TableIndex:             "v3",
				TableColumns:           []uint64{16245},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v3_idx",
				ReferencedTableColumns: []uint64{5237},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_Cascade,
				OnDelete:               doltdb.ForeignKeyReferentialAction_Cascade,
			},
			{
				Name:                   "fk4",
				TableName:              "sibling",
				TableIndex:             "v4",
				TableColumns:           []uint64{9036},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v4_idx",
				ReferencedTableColumns: []uint64{14774},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_SetNull,
				OnDelete:               doltdb.ForeignKeyReferentialAction_SetNull,
			},
			{
				Name:                   "fk5",
				TableName:              "sibling",
				TableIndex:             "v5",
				TableColumns:           []uint64{11586},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v5_idx",
				ReferencedTableColumns: []uint64{8125},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_NoAction,
				OnDelete:               doltdb.ForeignKeyReferentialAction_NoAction,
			},
		},
	},
	{
		name: "create foreign key without preexisting child index",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `alter table child add constraint child_fk foreign key (v1) references parent(v1)`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:      "child_fk",
				TableName: "child",
				// unnamed indexes take the column name
				TableIndex:             "v1",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
			},
		},
	},
	{
		name: "create unnamed foreign key",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v1_idx (v1)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child add foreign key (v1) references parent(v1)`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "19eof0mu",
				TableName:              "child",
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
			},
		},
	},
	{
		name: "create table with unnamed foreign key",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `create table new_table (
				id int,
				v1 int,
				foreign key (v1) references parent(v1),
				primary key(id));`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:      "mv9a59oo",
				TableName: "new_table",
				// unnamed indexes take the column name
				TableIndex:             "v1",
				TableColumns:           []uint64{7597},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
			},
		},
	},
	{
		name: "create unnamed multi-column foreign key",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `alter table parent add index v1v2_idx (v1, v2)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add index v1v2_idx (v1, v2)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add foreign key (v1, v2) references parent(v1, v2)`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "n4qun7ju",
				TableName:              "child",
				TableIndex:             "v1v2_idx",
				TableColumns:           []uint64{1215, 8734},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1v2_idx",
				ReferencedTableColumns: []uint64{6269, 7947},
			},
		},
	},
	{
		name: "create multiple unnamed foreign keys",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v1_idx (v1)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v2_idx (v2)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add foreign key (v1) references parent(v1)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add foreign key (v2) references parent(v2)`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "19eof0mu",
				TableName:              "child",
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
			},
			{
				Name:                   "p79c8qtq",
				TableName:              "child",
				TableIndex:             "v2_idx",
				TableColumns:           []uint64{8734},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v2_idx",
				ReferencedTableColumns: []uint64{7947},
			},
		},
	},
	{
		name: "create foreign key with pre-existing data",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `insert into parent (id,v1,v2) values 
				(1,1,1),
				(2,2,2);`}},
			{commands.SqlCmd{}, []string{"-q", `insert into child (id,v1,v2) values 
				(1,1,1),
				(2,2,2),
				(3,NULL,3);`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v1_idx (v1)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add constraint fk1 foreign key (v1) references parent(v1)`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "fk1",
				TableName:              "child",
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
			},
		},
	},
	{
		name: "create multi-col foreign key with pre-existing data",
		setup: []testCommand{
			{commands.SqlCmd{}, []string{"-q", `insert into parent (id,v1,v2) values 
				(1,1,1),
				(2,2,NULL),
				(3,NULL,3),
				(4,NULL,NULL);`}},
			{commands.SqlCmd{}, []string{"-q", `insert into child (id,v1,v2) values 
				(1,1,1),
				(2,2,NULL),
				(3,NULL,3);`}},
			{commands.SqlCmd{}, []string{"-q", `alter table parent add index v1v2 (v1,v2)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child add index v1v2 (v1,v2)`}},
			{commands.SqlCmd{}, []string{"-q", `alter table child 
				add constraint fk1 foreign key (v1,v2) references parent(v1,v2)`}},
		},
		fks: []doltdb.ForeignKey{
			{
				Name:                   "fk1",
				TableName:              "child",
				TableIndex:             "v1v2",
				TableColumns:           []uint64{1215, 8734},
				ReferencedTableName:    "parent",
				ReferencedTableIndex:   "v1v2",
				ReferencedTableColumns: []uint64{6269, 7947},
			},
		},
	},
}
