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

package doltdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func TestWorkingSetSchemaConflictRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name   string
		tables []TableName
	}{
		{"empty", []TableName{}},
		{"default schema", []TableName{{Name: "t"}, {Name: "u"}}},
		{"named schema", []TableName{{Name: "t", Schema: "s1"}}},
		{"same name in multiple schemas", []TableName{{Name: "t", Schema: "s2"}, {Name: "t"}, {Name: "t", Schema: "s1"}, {Name: "u", Schema: "s1"}}},
		{"dots in names", []TableName{{Name: "s1.t"}, {Name: "t", Schema: "s1"}, {Name: "t.u", Schema: "s.2"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ddb, ctx, head := newMemDoltDBWithDefaultBranch(t, "main")
			ours, err := head.GetRootValue(ctx)
			require.NoError(t, err)
			for _, schemaName := range []string{"s1", "s2", "s.2"} {
				ours, err = ours.CreateDatabaseSchema(ctx, schema.DatabaseSchema{Name: schemaName})
				require.NoError(t, err)
			}
			theirs := ours
			type tablePair struct{ ours, theirs hash.Hash }
			want := make(map[TableName]tablePair)
			for i, name := range test.tables {
				ourSchema := schema.MustSchemaFromCols(schema.NewColCollection(schema.NewColumn("id", uint64(i), types.IntKind, true)))
				theirSchema := schema.MustSchemaFromCols(schema.NewColCollection(schema.NewColumn("id", uint64(i+100), types.StringKind, true)))
				ourTable, err := NewEmptyTable(ctx, ddb.vrw, ddb.ns, ourSchema)
				require.NoError(t, err)
				theirTable, err := NewEmptyTable(ctx, ddb.vrw, ddb.ns, theirSchema)
				require.NoError(t, err)
				ours, err = ours.PutTable(ctx, name, ourTable)
				require.NoError(t, err)
				theirs, err = theirs.PutTable(ctx, name, theirTable)
				require.NoError(t, err)
				ourHash, err := ourTable.HashOf()
				require.NoError(t, err)
				theirHash, err := theirTable.HashOf()
				require.NoError(t, err)
				want[name] = tablePair{ourHash, theirHash}
			}

			meta, err := datas.NewCommitMeta("Test", "test@example.com", "source")
			require.NoError(t, err)
			heads, err := ddb.CommitHeadUpdates(ctx, []HeadUpdate{BranchHeadUpdate{
				HeadRef: ref.NewBranchRef("source"), Root: theirs, CommitOptions: datas.CommitOptions{Meta: meta},
			}}, nil)
			require.NoError(t, err)
			source, err := HashToCommit(ctx, ddb.vrw, ddb.ns, heads[0])
			require.NoError(t, err)
			wsRef := ref.NewWorkingSetRef("heads/conflicts")
			ws := EmptyWorkingSet(wsRef).WithWorkingRoot(ours).WithStagedRoot(ours).
				StartMerge(head, source, "source").WithUnmergableTables(test.tables)
			for cycle := 0; cycle < 2; cycle++ {
				prev, err := ws.HashOf()
				require.NoError(t, err)
				_, err = ddb.CommitHeadUpdates(ctx, []HeadUpdate{WorkingSetUpdate{
					WorkingSet: ws, PrevHash: prev, Meta: TodoWorkingSetMeta(),
				}}, nil)
				require.NoError(t, err)
				ws, err = ddb.ResolveWorkingSet(ctx, wsRef)
				require.NoError(t, err)
				require.Equal(t, test.tables, ws.MergeState().TablesWithSchemaConflicts(), "conflicting table identities changed after publication")
				var seen []TableName
				err = ws.MergeState().IterSchemaConflicts(ctx, ddb, func(name TableName, conflict SchemaConflict) error {
					seen = append(seen, name)
					require.NotNil(t, conflict.toTbl, "schema conflict destination table is missing")
					require.NotNil(t, conflict.fromTbl, "schema conflict source table is missing")
					ourHash, err := conflict.toTbl.HashOf()
					require.NoError(t, err)
					theirHash, err := conflict.fromTbl.HashOf()
					require.NoError(t, err)
					require.Equal(t, want[name], tablePair{ourHash, theirHash}, "schema conflict table pair changed after publication")
					return nil
				})
				require.NoError(t, err)
				require.Len(t, seen, len(test.tables), "schema conflict iteration lost tables")
			}
		})
	}
}
