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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestTableDiff(t *testing.T) {
	ddb, _ := LoadDoltDB(context.Background(), types.Format_7_18, InMemDoltDB)
	ddb.WriteEmptyRepo(context.Background(), "billy bob", "bigbillieb@fake.horse")

	cs, _ := NewCommitSpec("head", "master")
	cm, _ := ddb.Resolve(context.Background(), cs)

	root, err := cm.GetRootValue()
	assert.NoError(t, err)
	added, modified, removed, err := root.TableDiff(context.Background(), root)
	assert.NoError(t, err)

	if len(added)+len(modified)+len(removed) != 0 {
		t.Error("Bad table diff when comparing two repos")
	}

	sch := createTestSchema()
	m, err := types.NewMap(context.Background(), ddb.ValueReadWriter())
	assert.NoError(t, err)

	tbl1, err := createTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	root2, err := root.PutTable(context.Background(), "tbl1", tbl1)
	assert.NoError(t, err)

	added, modified, removed, err = root2.TableDiff(context.Background(), root)
	assert.NoError(t, err)

	if len(added) != 1 || added[0] != "tbl1" || len(modified)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	added, modified, removed, err = root.TableDiff(context.Background(), root2)
	assert.NoError(t, err)

	if len(removed) != 1 || removed[0] != "tbl1" || len(modified)+len(added) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	rowData, _ := createTestRowData(t, ddb.ValueReadWriter(), sch)
	tbl1Updated, _ := createTestTable(ddb.ValueReadWriter(), sch, rowData)

	root3, err := root.PutTable(context.Background(), "tbl1", tbl1Updated)
	assert.NoError(t, err)

	added, modified, removed, err = root3.TableDiff(context.Background(), root2)
	assert.NoError(t, err)

	if len(modified) != 1 || modified[0] != "tbl1" || len(added)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	added, modified, removed, err = root2.TableDiff(context.Background(), root3)
	assert.NoError(t, err)

	if len(modified) != 1 || modified[0] != "tbl1" || len(added)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	root4, err := root3.PutTable(context.Background(), "tbl2", tbl1)
	assert.NoError(t, err)

	added, modified, removed, err = root2.TableDiff(context.Background(), root4)
	assert.NoError(t, err)
	if len(modified) != 1 || modified[0] != "tbl1" || len(removed) != 1 || removed[0] != "tbl2" || +len(added) != 0 {
		t.Error("Bad table diff after adding a second table")
	}

	added, modified, removed, err = root4.TableDiff(context.Background(), root2)
	assert.NoError(t, err)
	if len(modified) != 1 || modified[0] != "tbl1" || len(added) != 1 || added[0] != "tbl2" || +len(removed) != 0 {
		t.Error("Bad table diff after adding a second table")
	}
}

func TestAddNewerTextAndValueFromTable(t *testing.T) {
	ddb, _ := LoadDoltDB(context.Background(), types.Format_7_18, InMemDoltDB)
	ddb.WriteEmptyRepo(context.Background(), "billy bob", "bigbillieb@fake.horse")

	doc1 := DocDetails{DocPk: LicensePk}
	doc1, err := AddNewerTextToDocFromTbl(context.Background(), nil, nil, doc1)
	assert.NoError(t, err)
	assert.Nil(t, doc1.NewerText)
	doc1, err = AddValueToDocFromTbl(context.Background(), nil, nil, doc1)
	assert.NoError(t, err)
	assert.Nil(t, doc1.Value)

	sch := createTestDocsSchema()

	rows := []row.Row{}
	m, _ := createTestRows(t, ddb.ValueReadWriter(), sch, rows)

	tbl, err := createTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	doc2 := DocDetails{DocPk: LicensePk}
	doc2, err = AddNewerTextToDocFromTbl(context.Background(), tbl, &sch, doc2)
	assert.NoError(t, err)
	assert.Nil(t, doc2.NewerText)
	doc2, err = AddValueToDocFromTbl(context.Background(), tbl, &sch, doc2)
	assert.NoError(t, err)
	assert.Nil(t, doc2.Value)

	doc3 := DocDetails{DocPk: LicensePk, NewerText: []byte("Something in newer text field")}
	doc3, err = AddNewerTextToDocFromTbl(context.Background(), tbl, &sch, doc3)
	assert.NoError(t, err)
	assert.Nil(t, doc3.NewerText)
	doc3, err = AddValueToDocFromTbl(context.Background(), tbl, &sch, doc3)
	assert.NoError(t, err)
	assert.Nil(t, doc3.Value)

	rows = getDocRows(t, sch, types.String("text in doc_text"))
	m, _ = createTestRows(t, ddb.ValueReadWriter(), sch, rows)
	tbl, err = createTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	doc4 := DocDetails{DocPk: LicensePk, NewerText: []byte("Something in newer text field")}
	doc4, err = AddNewerTextToDocFromTbl(context.Background(), tbl, &sch, doc4)
	assert.NoError(t, err)
	assert.Equal(t, "text in doc_text", string(doc4.NewerText))
	doc4, err = AddValueToDocFromTbl(context.Background(), tbl, &sch, doc4)
	assert.NoError(t, err)
	assert.Equal(t, types.String("text in doc_text"), doc4.Value)

	doc5 := DocDetails{DocPk: LicensePk}
	doc5, err = AddNewerTextToDocFromTbl(context.Background(), tbl, &sch, doc5)
	assert.NoError(t, err)
	assert.Equal(t, "text in doc_text", string(doc5.NewerText))
	doc5, err = AddValueToDocFromTbl(context.Background(), tbl, &sch, doc5)
	assert.NoError(t, err)
	assert.Equal(t, types.String("text in doc_text"), doc5.Value)
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
	row1, err := row.New(types.Format_7_18, sch, row.TaggedValues{
		DocNameTag: types.String(LicensePk),
		DocTextTag: rowVal,
	})
	rows[0] = row1
	assert.NoError(t, err)

	row2, err := row.New(types.Format_7_18, sch, row.TaggedValues{
		DocNameTag: types.String(ReadmePk),
		DocTextTag: rowVal,
	})
	rows[1] = row2
	assert.NoError(t, err)

	return rows
}

func createTestRows(t *testing.T, vrw types.ValueReadWriter, sch schema.Schema, rows []row.Row) (types.Map, []row.Row) {
	var err error

	m, err := types.NewMap(context.Background(), vrw)
	assert.NoError(t, err)
	ed := m.Edit()

	for _, r := range rows {
		ed = ed.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
	}

	m, err = ed.Map(context.Background())
	assert.NoError(t, err)

	return m, rows
}