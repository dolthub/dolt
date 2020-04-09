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

package doltdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestTableDiff(t *testing.T) {
	ctx := context.Background()
	ddb, _ := LoadDoltDB(ctx, types.Format_7_18, InMemDoltDB)
	ddb.WriteEmptyRepo(ctx, "billy bob", "bigbillieb@fake.horse")

	cs, _ := NewCommitSpec("head", "master")
	cm, _ := ddb.Resolve(ctx, cs)

	root, err := cm.GetRootValue()
	assert.NoError(t, err)
	added, modified, removed, err := root.TableDiff(ctx, root)
	assert.NoError(t, err)

	if len(added)+len(modified)+len(removed) != 0 {
		t.Error("Bad table diff when comparing two repos")
	}

	sch := createTestSchema()
	m, err := types.NewMap(ctx, ddb.ValueReadWriter())
	assert.NoError(t, err)

	tbl1, err := createTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	root2, err := root.PutTable(ctx, "tbl1", tbl1)
	assert.NoError(t, err)

	added, modified, removed, err = root2.TableDiff(ctx, root)
	assert.NoError(t, err)

	if len(added) != 1 || added[0] != "tbl1" || len(modified)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	added, modified, removed, err = root.TableDiff(ctx, root2)
	assert.NoError(t, err)

	if len(removed) != 1 || removed[0] != "tbl1" || len(modified)+len(added) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	rowData, _ := createTestRowData(t, ddb.ValueReadWriter(), sch)
	tbl1Updated, _ := createTestTable(ddb.ValueReadWriter(), sch, rowData)

	root3, err := root.PutTable(ctx, "tbl1", tbl1Updated)
	assert.NoError(t, err)

	added, modified, removed, err = root3.TableDiff(ctx, root2)
	assert.NoError(t, err)

	if len(modified) != 1 || modified[0] != "tbl1" || len(added)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	added, modified, removed, err = root2.TableDiff(ctx, root3)
	assert.NoError(t, err)

	if len(modified) != 1 || modified[0] != "tbl1" || len(added)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	cc, _ := schema.NewColCollection(
		schema.NewColumn("id", uint64(100), types.UUIDKind, true, schema.NotNullConstraint{}),
	)
	tbl2, err := createTestTable(ddb.ValueReadWriter(), schema.SchemaFromCols(cc), m)
	assert.NoError(t, err)

	root4, err := root3.PutTable(ctx, "tbl2", tbl2)
	assert.NoError(t, err)

	added, modified, removed, err = root2.TableDiff(ctx, root4)
	assert.NoError(t, err)
	if len(modified) != 1 || modified[0] != "tbl1" || len(removed) != 1 || removed[0] != "tbl2" || +len(added) != 0 {
		t.Error("Bad table diff after adding a second table")
	}

	added, modified, removed, err = root4.TableDiff(ctx, root2)
	assert.NoError(t, err)
	if len(modified) != 1 || modified[0] != "tbl1" || len(added) != 1 || added[0] != "tbl2" || +len(removed) != 0 {
		t.Error("Bad table diff after adding a second table")
	}
}

func TestDocDiff(t *testing.T) {
	ctx := context.Background()
	ddb, _ := LoadDoltDB(ctx, types.Format_7_18, InMemDoltDB)
	ddb.WriteEmptyRepo(ctx, "billy bob", "bigbillieb@fake.horse")

	cs, _ := NewCommitSpec("head", "master")
	cm, _ := ddb.Resolve(ctx, cs)

	root, err := cm.GetRootValue()
	assert.NoError(t, err)

	docDetails := []DocDetails{
		{DocPk: LicensePk},
		{DocPk: ReadmePk},
	}

	// DocDiff between a root and itself should return no added, modified or removed docs.
	added, modified, removed, err := root.DocDiff(ctx, root, docDetails)
	assert.NoError(t, err)

	if len(added)+len(modified)+len(removed) != 0 {
		t.Error("Bad doc diff when comparing two repos")
	}

	// Create tbl1 with one license row
	sch := createTestDocsSchema()
	licRow := getDocRow(t, sch, LicensePk, types.String("license row"))
	m, _ := createTestRows(t, ddb.ValueReadWriter(), sch, []row.Row{licRow})
	tbl1, err := createTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	// Create root2 with tbl1 on it (one doc: license)
	root2, err := root.PutTable(ctx, DocTableName, tbl1)
	assert.NoError(t, err)

	// DocDiff between root and root2 should return one added doc, LICENSE.md
	added, modified, removed, err = root.DocDiff(ctx, root2, docDetails)
	assert.NoError(t, err)

	if len(added) != 1 || added[0] != "LICENSE.md" || len(modified)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	// Create tbl2 with one readme row
	readmeRow := getDocRow(t, sch, ReadmePk, types.String("readme row"))
	m, _ = createTestRows(t, ddb.ValueReadWriter(), sch, []row.Row{readmeRow})
	tbl2, err := createTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	// Create root3 with tbl2 on it (one doc: readme)
	root3, err := root.PutTable(ctx, DocTableName, tbl2)
	assert.NoError(t, err)

	// DocDiff between root2 and root3 should return one removed doc (license) and one added doc (readme).
	added, modified, removed, err = root2.DocDiff(ctx, root3, docDetails)
	assert.NoError(t, err)

	if len(removed) != 1 || removed[0] != "LICENSE.md" || len(added) != 1 || added[0] != "README.md" || len(modified) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	// Create tbl3 with 2 doc rows (readme, license)
	readmeRowUpdated := getDocRow(t, sch, ReadmePk, types.String("a different readme"))
	m, _ = createTestRows(t, ddb.ValueReadWriter(), sch, []row.Row{readmeRowUpdated, licRow})
	tbl3, err := createTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	// Create root4 with tbl3 on it (two docs: readme and license)
	root4, err := root3.PutTable(ctx, DocTableName, tbl3)
	assert.NoError(t, err)

	// DocDiff between root3 and root4 should return one added doc (license) and one modified doc (readme).
	added, modified, removed, err = root3.DocDiff(ctx, root4, nil)
	assert.NoError(t, err)

	if len(added) != 1 || added[0] != "LICENSE.md" || len(modified) != 1 || modified[0] != "README.md" || len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	// DocDiff between root4 and root shows 2 remove docs (license, readme)
	added, modified, removed, err = root4.DocDiff(ctx, root, nil)
	assert.NoError(t, err)

	if len(removed) != 2 || len(modified) != 0 || len(added) != 0 {
		t.Error("Bad table diff after adding a single table")
	}
}

func TestAddNewerTextAndValueFromTable(t *testing.T) {
	ctx := context.Background()
	ddb, _ := LoadDoltDB(ctx, types.Format_7_18, InMemDoltDB)
	ddb.WriteEmptyRepo(ctx, "billy bob", "bigbillieb@fake.horse")

	// If no tbl/schema is provided, doc NewerText and Value should be nil.
	doc1 := DocDetails{DocPk: LicensePk}
	doc1, err := AddNewerTextToDocFromTbl(ctx, nil, nil, doc1)
	assert.NoError(t, err)
	assert.Nil(t, doc1.NewerText)
	doc1, err = AddValueToDocFromTbl(ctx, nil, nil, doc1)
	assert.NoError(t, err)
	assert.Nil(t, doc1.Value)

	// Create table with no rows
	sch := createTestDocsSchema()
	rows := []row.Row{}
	m, _ := createTestRows(t, ddb.ValueReadWriter(), sch, rows)
	tbl, err := createTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	// If a table doesn't have doc row, doc NewerText and Value should remain nil
	doc2 := DocDetails{DocPk: LicensePk}
	doc2, err = AddNewerTextToDocFromTbl(ctx, tbl, &sch, doc2)
	assert.NoError(t, err)
	assert.Nil(t, doc2.NewerText)
	doc2, err = AddValueToDocFromTbl(ctx, tbl, &sch, doc2)
	assert.NoError(t, err)
	assert.Nil(t, doc2.Value)

	// If a table doesn't have doc row, and NewerText and Value are originally non-nil, they should be updated to nil.
	doc3 := DocDetails{DocPk: LicensePk, NewerText: []byte("Something in newer text field"), Value: types.String("something")}
	doc3, err = AddNewerTextToDocFromTbl(ctx, tbl, &sch, doc3)
	assert.NoError(t, err)
	assert.Nil(t, doc3.NewerText)
	doc3, err = AddValueToDocFromTbl(ctx, tbl, &sch, doc3)
	assert.NoError(t, err)
	assert.Nil(t, doc3.Value)

	// Update tbl to have 2 doc rows, readme and license
	rows = getDocRows(t, sch, types.String("text in doc_text"))
	m, _ = createTestRows(t, ddb.ValueReadWriter(), sch, rows)
	tbl, err = createTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	// If a table has a doc row, NewerText and Value and should be updated to the `doc_text` value in that row.
	doc4 := DocDetails{DocPk: LicensePk, NewerText: []byte("Something in newer text field")}
	doc4, err = AddNewerTextToDocFromTbl(ctx, tbl, &sch, doc4)
	assert.NoError(t, err)
	assert.Equal(t, "text in doc_text", string(doc4.NewerText))
	doc4, err = AddValueToDocFromTbl(ctx, tbl, &sch, doc4)
	assert.NoError(t, err)
	assert.Equal(t, types.String("text in doc_text"), doc4.Value)

	// If a table has a doc row, and NewerText and Value are originally non-nil, they should be updated to the `doc_text` value.
	doc5 := DocDetails{DocPk: LicensePk}
	doc5, err = AddNewerTextToDocFromTbl(ctx, tbl, &sch, doc5)
	assert.NoError(t, err)
	assert.Equal(t, "text in doc_text", string(doc5.NewerText))
	doc5, err = AddValueToDocFromTbl(ctx, tbl, &sch, doc5)
	assert.NoError(t, err)
	assert.Equal(t, types.String("text in doc_text"), doc5.Value)
}

func TestAddNewerTextAndDocPkFromRow(t *testing.T) {
	ctx := context.Background()
	ddb, _ := LoadDoltDB(ctx, types.Format_7_18, InMemDoltDB)
	ddb.WriteEmptyRepo(ctx, "billy bob", "bigbillieb@fake.horse")

	sch := createTestDocsSchema()

	emptyRow, err := row.New(types.Format_7_18, sch, row.TaggedValues{})

	// NewerText and DocPk should be nil from an empty row
	doc1 := DocDetails{}
	doc1, err = addNewerTextToDocFromRow(ctx, emptyRow, &doc1)
	assert.NoError(t, err)
	assert.Nil(t, doc1.NewerText)
	doc1, err = addDocPKToDocFromRow(emptyRow, &doc1)
	assert.NoError(t, err)
	assert.Equal(t, "", doc1.DocPk)

	licenseRow, err := row.New(types.Format_7_18, sch, row.TaggedValues{
		DocNameTag: types.String(LicensePk),
		DocTextTag: types.String("license!"),
	})
	assert.NoError(t, err)

	// NewerText and DocPk should be added to doc from row
	doc2 := DocDetails{}
	doc2, err = addNewerTextToDocFromRow(ctx, licenseRow, &doc2)
	assert.NoError(t, err)
	assert.Equal(t, "license!", string(doc2.NewerText))
	doc1, err = addDocPKToDocFromRow(licenseRow, &doc2)
	assert.NoError(t, err)
	assert.Equal(t, LicensePk, doc2.DocPk)

	// When NewerText and DocPk are non-nil, they should be updated from the row provided.
	doc3 := DocDetails{DocPk: "invalid", NewerText: []byte("something")}
	doc3, err = addNewerTextToDocFromRow(ctx, licenseRow, &doc3)
	assert.NoError(t, err)
	assert.Equal(t, "license!", string(doc3.NewerText))
	doc3, err = addDocPKToDocFromRow(licenseRow, &doc3)
	assert.NoError(t, err)
	assert.Equal(t, LicensePk, doc3.DocPk)
}

func createTestDocsSchema() schema.Schema {
	typedColColl, _ := schema.NewColCollection(
		schema.NewColumn(DocPkColumnName, DocNameTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn(DocTextColumnName, DocTextTag, types.StringKind, false),
	)
	return schema.SchemaFromCols(typedColColl)
}

func getDocRows(t *testing.T, sch schema.Schema, rowVal types.Value) []row.Row {
	rows := make([]row.Row, 2)
	row1 := getDocRow(t, sch, LicensePk, rowVal)
	rows[0] = row1
	row2 := getDocRow(t, sch, ReadmePk, rowVal)
	rows[1] = row2

	return rows
}

func getDocRow(t *testing.T, sch schema.Schema, pk string, rowVal types.Value) row.Row {
	row, err := row.New(types.Format_7_18, sch, row.TaggedValues{
		DocNameTag: types.String(pk),
		DocTextTag: rowVal,
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
