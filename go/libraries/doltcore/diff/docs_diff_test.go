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

package diff

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	filesys2 "github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func TestDocDiff(t *testing.T) {
	ctx := context.Background()
	ddb, _ := doltdb.LoadDoltDB(ctx, types.Format_Default, doltdb.InMemDoltDB, filesys2.LocalFS)
	ddb.WriteEmptyRepo(ctx, env.DefaultInitBranch, "billy bob", "bigbillieb@fake.horse")

	cs, _ := doltdb.NewCommitSpec(env.DefaultInitBranch)
	cm, _ := ddb.Resolve(ctx, cs, nil)

	root, err := cm.GetRootValue()
	assert.NoError(t, err)

	docs := doltdocs.Docs{
		{DocPk: doltdocs.LicenseDoc},
		{DocPk: doltdocs.ReadmeDoc},
	}

	// DocsDiff between a root and itself should return no added, modified or removed docs.
	added, modified, removed, err := DocsDiff(ctx, root, root, docs)
	assert.NoError(t, err)

	if len(added)+len(modified)+len(removed) != 0 {
		t.Error("Bad doc diff when comparing two repos")
	}

	// Create tbl1 with one license row
	sch := createTestDocsSchema()
	licRow := makeDocRow(t, sch, doltdocs.LicenseDoc, types.String("license row"))
	m, _ := createTestRows(t, ddb.ValueReadWriter(), sch, []row.Row{licRow})
	tbl1, err := CreateTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	// Create root2 with tbl1 on it (one doc: license)
	root2, err := root.PutTable(ctx, doltdb.DocTableName, tbl1)
	assert.NoError(t, err)

	// DocsDiff between root and root2 should return one added doc, LICENSE.md
	added, modified, removed, err = DocsDiff(ctx, root, root2, docs)
	assert.NoError(t, err)

	if len(added) != 1 || added[0] != "LICENSE.md" || len(modified)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	// Create tbl2 with one readme row
	readmeRow := makeDocRow(t, sch, doltdocs.ReadmeDoc, types.String("readme row"))
	m, _ = createTestRows(t, ddb.ValueReadWriter(), sch, []row.Row{readmeRow})
	tbl2, err := CreateTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	// Create root3 with tbl2 on it (one doc: readme)
	root3, err := root.PutTable(ctx, doltdb.DocTableName, tbl2)
	assert.NoError(t, err)

	// DocsDiff between root2 and root3 should return one removed doc (license) and one added doc (readme).
	added, modified, removed, err = DocsDiff(ctx, root2, root3, docs)
	assert.NoError(t, err)

	if len(removed) != 1 || removed[0] != "LICENSE.md" || len(added) != 1 || added[0] != "README.md" || len(modified) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	// Create tbl3 with 2 doc rows (readme, license)
	readmeRowUpdated := makeDocRow(t, sch, doltdocs.ReadmeDoc, types.String("a different readme"))
	m, _ = createTestRows(t, ddb.ValueReadWriter(), sch, []row.Row{readmeRowUpdated, licRow})
	tbl3, err := CreateTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	// Create root4 with tbl3 on it (two docs: readme and license)
	root4, err := root3.PutTable(ctx, doltdb.DocTableName, tbl3)
	assert.NoError(t, err)

	// DocsDiff between root3 and root4 should return one added doc (license) and one modified doc (readme).
	added, modified, removed, err = DocsDiff(ctx, root3, root4, nil)
	assert.NoError(t, err)

	if len(added) != 1 || added[0] != "LICENSE.md" || len(modified) != 1 || modified[0] != "README.md" || len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	// DocsDiff between root4 and root shows 2 remove docs (license, readme)
	added, modified, removed, err = DocsDiff(ctx, root4, root, nil)
	assert.NoError(t, err)

	if len(removed) != 2 || len(modified) != 0 || len(added) != 0 {
		t.Error("Bad table diff after adding a single table")
	}
}

func CreateTestTable(vrw types.ValueReadWriter, tSchema schema.Schema, rowData types.Map) (*doltdb.Table, error) {
	tbl, err := doltdb.NewNomsTable(context.Background(), vrw, tSchema, rowData, nil, nil)

	if err != nil {
		return nil, err
	}

	return tbl, nil
}

func createTestDocsSchema() schema.Schema {
	typedColColl := schema.NewColCollection(
		schema.NewColumn(doltdb.DocPkColumnName, schema.DocNameTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn(doltdb.DocTextColumnName, schema.DocTextTag, types.StringKind, false),
	)
	sch, err := schema.SchemaFromCols(typedColColl)
	if err != nil {
		panic(err)
	}
	return sch
}

func makeDocRow(t *testing.T, sch schema.Schema, pk string, rowVal types.Value) row.Row {
	row, err := row.New(types.Format_Default, sch, row.TaggedValues{
		schema.DocNameTag: types.String(pk),
		schema.DocTextTag: rowVal,
	})
	assert.NoError(t, err)

	return row
}

func createTestRows(t *testing.T, vrw types.ValueReadWriter, sch schema.Schema, rows []row.Row) (types.Map, []row.Row) {
	ctx := context.Background()
	var err error

	m, err := types.NewMap(ctx, vrw)
	assert.NoError(t, err)
	ed := m.Edit()

	for _, r := range rows {
		ed = ed.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
	}

	m, err = ed.Map(ctx)
	assert.NoError(t, err)

	return m, rows
}
