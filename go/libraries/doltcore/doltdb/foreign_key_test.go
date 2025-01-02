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
)

func TestForeignKeyHashOf(t *testing.T) {
	// Assert that we can get an expected hash for a simple Foreign Key
	t.Run("HashOf for resolved foreign key", func(t *testing.T) {
		fk := doltdb.ForeignKey{
			Name:                   "fk1",
			TableName:              doltdb.TableName{Name: "table1"},
			TableIndex:             "i1",
			TableColumns:           []uint64{123},
			ReferencedTableName:    doltdb.TableName{Name: "parentTable"},
			ReferencedTableIndex:   "i2",
			ReferencedTableColumns: []uint64{321},
			OnUpdate:               0,
			OnDelete:               0,
			UnresolvedFKDetails:    doltdb.UnresolvedFKDetails{},
		}
		hashOf, err := fk.HashOf()
		assert.NoError(t, err)
		assert.Equal(t, "65brfkb3fh6n7kgpv8d38mjb6krrc54r", hashOf.String())
	})

	// Assert that two unresolved Foreign Keys get unique hashes, when only their unresolved FK details are different
	t.Run("HashOf for unresolved FK uses unresolved fields", func(t *testing.T) {
		fk1 := doltdb.ForeignKey{
			Name:                   "",
			TableName:              doltdb.TableName{Name: "table1"},
			TableIndex:             "i1",
			TableColumns:           nil,
			ReferencedTableName:    doltdb.TableName{Name: "parentTable"},
			ReferencedTableIndex:   "i2",
			ReferencedTableColumns: nil,
			OnUpdate:               0,
			OnDelete:               0,
			UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
				TableColumns:           []string{"col1"},
				ReferencedTableColumns: []string{"col2"},
			},
		}
		hash1, err := fk1.HashOf()
		assert.NoError(t, err)
		assert.Equal(t, "qiv9l4juuk20buqml2unlbohfvo95mcd", hash1.String())

		// Create a second FK that is identical to fk1, except for the unresolved FK details to
		// assert that the UnresolvedFKDetails fields are used in the hash.
		fk2 := doltdb.ForeignKey{
			Name:                   "",
			TableName:              doltdb.TableName{Name: "table1"},
			TableIndex:             "i1",
			TableColumns:           nil,
			ReferencedTableName:    doltdb.TableName{Name: "parentTable"},
			ReferencedTableIndex:   "i2",
			ReferencedTableColumns: nil,
			OnUpdate:               0,
			OnDelete:               0,
			UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
				TableColumns:           []string{"col2"},
				ReferencedTableColumns: []string{"col2"},
			},
		}
		hash2, err := fk2.HashOf()
		assert.NoError(t, err)
		assert.Equal(t, "cdglg27qlu0dva6k87vriasnn11o2bnn", hash2.String())
		assert.NotEqual(t, hash1, hash2)
	})
}

func TestForeignKeys(t *testing.T) {
	for _, test := range foreignKeyTests {
		t.Run(test.name, func(t *testing.T) {
			testForeignKeys(t, test)
		})
	}
}

func TestForeignKeyErrors(t *testing.T) {
	cmds := []testCommand{
		{commands.SqlCmd{}, []string{"-q", `CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));`}},
		{commands.SqlCmd{}, []string{"-q", `CREATE TABLE test2(pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1),` +
			`CONSTRAINT child_fk FOREIGN KEY (v1) REFERENCES test(v1));`}},
	}

	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	cliCtx, err := commands.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, err)

	for _, c := range cmds {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	exitCode := commands.SqlCmd{}.Exec(ctx, commands.SqlCmd{}.Name(), []string{"-q", `ALTER TABLE test MODIFY v1 INT;`}, dEnv, cliCtx)
	require.Equal(t, 1, exitCode)
	exitCode = commands.SqlCmd{}.Exec(ctx, commands.SqlCmd{}.Name(), []string{"-q", `ALTER TABLE test2 MODIFY v1 INT;`}, dEnv, cliCtx)

	require.Equal(t, 1, exitCode)
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

	cliCtx, verr := commands.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	require.NoError(t, verr)

	for _, c := range fkSetupCommon {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}
	for _, c := range test.setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv, cliCtx)
		require.Equal(t, 0, exitCode)
	}

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	fkc, err := root.GetForeignKeyCollection(ctx)
	require.NoError(t, err)

	assert.Equal(t, test.fks, fkc.AllKeys())

	for _, fk := range test.fks {
		// verify parent index
		pt, _, ok, err := doltdb.GetTableInsensitive(ctx, root, fk.ReferencedTableName)
		require.NoError(t, err)
		require.True(t, ok)
		ps, err := pt.GetSchema(ctx)
		require.NoError(t, err)
		pi, ok := ps.Indexes().GetByNameCaseInsensitive(fk.ReferencedTableIndex)
		require.True(t, ok)
		require.Equal(t, fk.ReferencedTableColumns, pi.IndexedColumnTags())

		// verify child index
		ct, _, ok, err := doltdb.GetTableInsensitive(ctx, root, fk.TableName)
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
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
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
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v1v2_idx",
				TableColumns:           []uint64{1215, 8734},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1v2_idx",
				ReferencedTableColumns: []uint64{6269, 7947},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1", "v2"},
					ReferencedTableColumns: []string{"v1", "v2"},
				},
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
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
			},
			{
				Name:                   "fk2",
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v2_idx",
				TableColumns:           []uint64{8734},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v2_idx",
				ReferencedTableColumns: []uint64{7947},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v2"},
					ReferencedTableColumns: []string{"v2"},
				},
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
				TableName: doltdb.TableName{Name: "new_table"},
				// FK created indexes use the supplied FK name
				TableIndex:             "new_fk",
				TableColumns:           []uint64{7597},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
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
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_Cascade,
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
			},
			{
				Name:                   "fk2",
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v2_idx",
				TableColumns:           []uint64{8734},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v2_idx",
				ReferencedTableColumns: []uint64{7947},
				OnDelete:               doltdb.ForeignKeyReferentialAction_SetNull,
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v2"},
					ReferencedTableColumns: []string{"v2"},
				},
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
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_Cascade,
				OnDelete:               doltdb.ForeignKeyReferentialAction_Cascade,
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
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
				TableName:              doltdb.TableName{Name: "sibling"},
				TableIndex:             "fk1",
				TableColumns:           []uint64{16080},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
			},
			{
				Name:                   "fk2",
				TableName:              doltdb.TableName{Name: "sibling"},
				TableIndex:             "fk2",
				TableColumns:           []uint64{7576},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v2_idx",
				ReferencedTableColumns: []uint64{7947},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_Restrict,
				OnDelete:               doltdb.ForeignKeyReferentialAction_Restrict,
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v2"},
					ReferencedTableColumns: []string{"v2"},
				},
			},
			{
				Name:                   "fk3",
				TableName:              doltdb.TableName{Name: "sibling"},
				TableIndex:             "fk3",
				TableColumns:           []uint64{16245},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v3_idx",
				ReferencedTableColumns: []uint64{5237},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_Cascade,
				OnDelete:               doltdb.ForeignKeyReferentialAction_Cascade,
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v3"},
					ReferencedTableColumns: []string{"v3"},
				},
			},
			{
				Name:                   "fk4",
				TableName:              doltdb.TableName{Name: "sibling"},
				TableIndex:             "fk4",
				TableColumns:           []uint64{9036},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v4_idx",
				ReferencedTableColumns: []uint64{14774},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_SetNull,
				OnDelete:               doltdb.ForeignKeyReferentialAction_SetNull,
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v4"},
					ReferencedTableColumns: []string{"v4"},
				},
			},
			{
				Name:                   "fk5",
				TableName:              doltdb.TableName{Name: "sibling"},
				TableIndex:             "fk5",
				TableColumns:           []uint64{11586},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v5_idx",
				ReferencedTableColumns: []uint64{8125},
				OnUpdate:               doltdb.ForeignKeyReferentialAction_NoAction,
				OnDelete:               doltdb.ForeignKeyReferentialAction_NoAction,
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v5"},
					ReferencedTableColumns: []string{"v5"},
				},
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
				TableName: doltdb.TableName{Name: "child"},
				// FK created indexes use the supplied FK name
				TableIndex:             "child_fk",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
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
				Name:                   "child_ibfk_1",
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
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
				Name:      "new_table_ibfk_1",
				TableName: doltdb.TableName{Name: "new_table"},
				// unnamed indexes take the column name
				TableIndex:             "v1",
				TableColumns:           []uint64{7597},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
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
				Name:                   "child_ibfk_1",
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v1v2_idx",
				TableColumns:           []uint64{1215, 8734},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1v2_idx",
				ReferencedTableColumns: []uint64{6269, 7947},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1", "v2"},
					ReferencedTableColumns: []string{"v1", "v2"},
				},
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
				Name:                   "child_ibfk_1",
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
			},
			{
				Name:                   "child_ibfk_2",
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v2_idx",
				TableColumns:           []uint64{8734},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v2_idx",
				ReferencedTableColumns: []uint64{7947},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v2"},
					ReferencedTableColumns: []string{"v2"},
				},
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
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v1_idx",
				TableColumns:           []uint64{1215},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1_idx",
				ReferencedTableColumns: []uint64{6269},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1"},
					ReferencedTableColumns: []string{"v1"},
				},
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
				TableName:              doltdb.TableName{Name: "child"},
				TableIndex:             "v1v2",
				TableColumns:           []uint64{1215, 8734},
				ReferencedTableName:    doltdb.TableName{Name: "parent"},
				ReferencedTableIndex:   "v1v2",
				ReferencedTableColumns: []uint64{6269, 7947},
				UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
					TableColumns:           []string{"v1", "v2"},
					ReferencedTableColumns: []string{"v1", "v2"},
				},
			},
		},
	},
}
