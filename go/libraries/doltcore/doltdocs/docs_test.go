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

package doltdocs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/store/types"
)

func TestAddNewerTextAndValueFromTable(t *testing.T) {
	ctx := context.Background()
	ddb, _ := doltdb.LoadDoltDB(ctx, types.Format_7_18, doltdb.InMemDoltDB)
	ddb.WriteEmptyRepo(ctx, "billy bob", "bigbillieb@fake.horse")

	// If no tbl/schema is provided, doc Text and Value should be nil.
	doc1 := Doc{DocPk: doltdb.LicensePk}
	doc1Text, err := GetDocTextFromTbl(ctx, nil, nil, doc1.DocPk)
	assert.NoError(t, err)
	doc1.Text = doc1Text
	assert.Nil(t, doc1.Text)

	// Create table with no rows
	sch := createTestDocsSchema()
	rows := []row.Row{}
	m, _ := createTestRows(t, ddb.ValueReadWriter(), sch, rows)
	tbl, err := CreateTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	// If a table doesn't have doc row, doc Text and Value should remain nil
	doc2 := Doc{DocPk: doltdb.LicensePk}
	doc2Text, err := GetDocTextFromTbl(ctx, tbl, &sch, doc2.DocPk)
	assert.NoError(t, err)
	doc2.Text = doc2Text
	assert.Nil(t, doc2.Text)

	// If a table doesn't have doc row, and Text and Value are originally non-nil, they should be updated to nil.
	doc3 := Doc{DocPk: doltdb.LicensePk, Text: []byte("Something in newer text field")}
	doc3Text, err := GetDocTextFromTbl(ctx, tbl, &sch, doc3.DocPk)
	assert.NoError(t, err)
	doc3.Text = doc3Text
	assert.Nil(t, doc3.Text)

	// Update tbl to have 2 doc rows, readme and license
	rows = getDocRows(t, sch, types.String("text in doc_text"))
	m, _ = createTestRows(t, ddb.ValueReadWriter(), sch, rows)
	tbl, err = CreateTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	// If a table has a doc row, Text and Value and should be updated to the `doc_text` value in that row.
	doc4 := Doc{DocPk: doltdb.LicensePk, Text: []byte("Something in newer text field")}
	doc4Text, err := GetDocTextFromTbl(ctx, tbl, &sch, doc4.DocPk)
	assert.NoError(t, err)
	doc4.Text = doc4Text
	assert.Equal(t, "text in doc_text", string(doc4.Text))

	// If a table has a doc row, and Text and Value are originally non-nil, they should be updated to the `doc_text` value.
	doc5 := Doc{DocPk: doltdb.LicensePk}
	doc5Text, err := GetDocTextFromTbl(ctx, tbl, &sch, doc5.DocPk)
	assert.NoError(t, err)
	doc5.Text = doc5Text
	assert.Equal(t, "text in doc_text", string(doc5.Text))
}

func TestAddNewerTextAndDocPkFromRow(t *testing.T) {
	ctx := context.Background()
	ddb, _ := doltdb.LoadDoltDB(ctx, types.Format_7_18, doltdb.InMemDoltDB)
	ddb.WriteEmptyRepo(ctx, "billy bob", "bigbillieb@fake.horse")

	sch := createTestDocsSchema()

	emptyRow, err := row.New(types.Format_7_18, sch, row.TaggedValues{})

	// Text and DocName should be nil from an empty row
	doc1 := Doc{}
	text, err := GetDocTextFromRow(emptyRow)
	assert.NoError(t, err)
	assert.Nil(t, text)
	docPk, err := GetDocPKFromRow(emptyRow)
	assert.NoError(t, err)
	doc1.DocPk = docPk
	assert.Equal(t, "", doc1.DocPk)

	licenseRow, err := row.New(types.Format_7_18, sch, row.TaggedValues{
		schema.DocNameTag: types.String(doltdb.LicensePk),
		schema.DocTextTag: types.String("license!"),
	})
	assert.NoError(t, err)

	// Text and DocName should be added to doc from row
	doc2 := Doc{}
	text, err = GetDocTextFromRow(licenseRow)
	assert.NoError(t, err)
	doc2.Text = text
	assert.Equal(t, "license!", string(doc2.Text))
	docPk, err = GetDocPKFromRow(licenseRow)
	assert.NoError(t, err)
	doc2.DocPk = docPk
	assert.Equal(t, doltdb.LicensePk, doc2.DocPk)

	// When Text and DocName are non-nil, they should be updated from the row provided.
	doc3 := Doc{DocPk: "invalid", Text: []byte("something")}
	text, err = GetDocTextFromRow(licenseRow)
	assert.NoError(t, err)
	doc3.Text = text
	assert.Equal(t, "license!", string(doc3.Text))
	docPk, err = GetDocPKFromRow(licenseRow)
	assert.NoError(t, err)
	doc3.DocPk = docPk
	assert.Equal(t, doltdb.LicensePk, doc3.DocPk)
}

func CreateTestTable(vrw types.ValueReadWriter, tSchema schema.Schema, rowData types.Map) (*doltdb.Table, error) {
	schemaVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), vrw, tSchema)

	if err != nil {
		return nil, err
	}

	empty, _ := types.NewMap(context.Background(), vrw)
	tbl, err := doltdb.NewTable(context.Background(), vrw, schemaVal, rowData, empty)

	if err != nil {
		return nil, err
	}

	return tbl, nil
}

func createTestDocsSchema() schema.Schema {
	typedColColl, _ := schema.NewColCollection(
		schema.NewColumn(doltdb.DocPkColumnName, schema.DocNameTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn(doltdb.DocTextColumnName, schema.DocTextTag, types.StringKind, false),
	)
	sch, err := schema.SchemaFromCols(typedColColl)
	if err != nil {
		panic(err)
	}
	return sch
}

func getDocRows(t *testing.T, sch schema.Schema, rowVal types.Value) []row.Row {
	rows := make([]row.Row, 2)
	row1 := makeDocRow(t, sch, doltdb.LicensePk, rowVal)
	rows[0] = row1
	row2 := makeDocRow(t, sch, doltdb.ReadmePk, rowVal)
	rows[1] = row2

	return rows
}

func makeDocRow(t *testing.T, sch schema.Schema, pk string, rowVal types.Value) row.Row {
	row, err := row.New(types.Format_7_18, sch, row.TaggedValues{
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
