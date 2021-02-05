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
	"errors"
	"fmt"
	"strconv"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

// updateDocsTable takes in docTbl param and updates it with the value in docs. It returns the updated table.
func updateDocsTable(ctx context.Context, docTbl *doltdb.Table, docs Docs) (*doltdb.Table, error) {
	m, err := docTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := docTbl.GetSchema(context.Background())
	if err != nil {
		return nil, err
	}

	me := m.Edit()
	for _, doc := range docs {
		key, err := docTblKeyFromName(docTbl.Format(), doc.DocPk)
		if err != nil {
			return nil, err
		}

		docRow, exists, err := table.GetRow(ctx, docTbl, sch, key)
		if err != nil {
			return nil, err
		}

		if exists && doc.Text == nil {
			me = me.Remove(docRow.NomsMapKey(sch))
		} else if doc.Text != nil {
			docTaggedVals := row.TaggedValues{
				schema.DocNameTag: types.String(doc.DocPk),
				schema.DocTextTag: types.String(doc.Text),
			}
			docRow, err = row.New(types.Format_7_18, sch, docTaggedVals)
			if err != nil {
				return nil, err
			}
			me = me.Set(docRow.NomsMapKey(sch), docRow.NomsMapValue(sch))
		}
	}
	updatedMap, err := me.Map(ctx)
	if updatedMap.Len() == 0 {
		return nil, ErrEmptyDocsTable
	}

	docTbl, err = docTbl.UpdateRows(ctx, updatedMap)

	return docTbl, err
}

// createDocsTable creates a new in memory table that stores the given doc details.
func createDocsTable(ctx context.Context, vrw types.ValueReadWriter, docs Docs) (*doltdb.Table, error) {
	imt := table.NewInMemTable(Schema)

	// Determines if the table needs to be created at all and initializes a schema if it does.
	createTable := false
	for _, doc := range docs {
		if doc.Text != nil {
			createTable = true
			docTaggedVals := row.TaggedValues{
				schema.DocNameTag: types.String(doc.DocPk),
				schema.DocTextTag: types.String(doc.Text),
			}

			docRow, err := row.New(types.Format_7_18, Schema, docTaggedVals)
			if err != nil {
				return nil, err
			}
			err = imt.AppendRow(docRow)
			if err != nil {
				return nil, err
			}
		}
	}

	if createTable {
		rd := table.NewInMemTableReader(imt)
		wr := noms.NewNomsMapCreator(context.Background(), vrw, Schema)

		_, _, err := table.PipeRows(context.Background(), rd, wr, false)
		if err != nil {
			return nil, err
		}
		rd.Close(context.Background())
		wr.Close(context.Background())

		schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, wr.GetSchema())

		if err != nil {
			return nil, ErrMarshallingSchema
		}

		empty, err := types.NewMap(ctx, vrw)
		if err != nil {
			return nil, err
		}

		newDocsTbl, err := doltdb.NewTable(ctx, vrw, schVal, wr.GetMap(), empty, nil)
		if err != nil {
			return nil, err
		}

		return newDocsTbl, nil
	}

	return nil, nil
}

// CreateOrUpdateDocsTable takes a root value and a set of docs and either creates the docs table or updates it with docs.
func CreateOrUpdateDocsTable(ctx context.Context, root *doltdb.RootValue, docs Docs) (*doltdb.Table, error) {
	docsTbl, found, err := root.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return nil, err
	}

	if found {
		return updateDocsTable(ctx, docsTbl, docs)
	} else {
		return createDocsTable(ctx, root.VRW(), docs)
	}
}

func docTblKeyFromName(fmt *types.NomsBinFormat, name string) (types.Tuple, error) {
	return types.NewTuple(fmt, types.Uint(schema.DocNameTag), types.String(name))
}

// getDocTextFromTbl returns the Text field of a doc using the provided table and schema and primary key.
func getDocTextFromTbl(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, docPk string) ([]byte, error) {
	if tbl != nil && sch != nil {
		key, err := docTblKeyFromName(tbl.Format(), docPk)
		if err != nil {
			return nil, err
		}

		docRow, ok, err := getDocRow(ctx, tbl, *sch, key)
		if err != nil {
			return nil, err
		}
		if ok {
			docValue, _ := docRow.GetColVal(schema.DocTextTag)
			return []byte(docValue.(types.String)), nil
		} else {
			return nil, nil
		}
	} else {
		return nil, nil
	}
}

// getDocRow returns the associated row of a particular doc from the docTbl given.
func getDocRow(ctx context.Context, docTbl *doltdb.Table, sch schema.Schema, key types.Tuple) (r row.Row, ok bool, err error) {
	rowMap, err := docTbl.GetRowData(ctx)
	if err != nil {
		return nil, false, err
	}

	var fields types.Value
	fields, ok, err = rowMap.MaybeGet(ctx, key)
	if err != nil || !ok {
		return nil, ok, err
	}

	r, err = row.FromNoms(sch, key, fields.(types.Tuple))
	return r, ok, err
}

// getDocTextFromRow updates return the text field of a provided row.
func getDocTextFromRow(r row.Row) ([]byte, error) {
	docValue, ok := r.GetColVal(schema.DocTextTag)
	if !ok {
		return nil, nil
	} else {
		docValStr, err := strconv.Unquote(docValue.HumanReadableString())
		if err != nil {
			return nil, err
		}
		return []byte(docValStr), nil
	}
}

// getDocPKFromRow updates returns the docPk field of a given row.
func getDocPKFromRow(r row.Row) (string, error) {
	colVal, _ := r.GetColVal(schema.DocNameTag)
	if colVal == nil {
		return "", nil
	} else {
		docName, err := strconv.Unquote(colVal.HumanReadableString())
		if err != nil {
			return "", err
		}

		return docName, nil
	}
}

// getFileFromDoc returns the file obj associated with the doc
func getFileFromDoc(docName string) (string, error) {
	if doc, ok := IsSupportedDoc(docName); ok {
		return doc.File, nil
	}

	return "", fmt.Errorf("Doc name not provided %s", docName)
}

// GetAllDocs takes a root value and returns all the docs available in the root.
func GetAllDocs(ctx context.Context, root *doltdb.RootValue) (Docs, bool, error) {
	if root == nil {
		return nil, false, nil
	}

	docsTbl, found, err := root.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return nil, false, err
	}

	if !found {
		return nil, false, err
	}

	docs, err := getDocsFromTable(ctx, docsTbl)
	return docs, true, err
}

// getDocsFromTable takes the doltdocs table and a schema and return all docs in the dolt_docs table.
func getDocsFromTable(ctx context.Context, table *doltdb.Table) (Docs, error) {
	ret := make(Docs, 0)

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	err = rows.IterAll(ctx, func(key, val types.Value) error {
		newRow, err := row.FromNoms(sch, key.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return err
		}

		cols := sch.GetAllCols().GetColumns()
		colVals := make([]types.Value, len(cols))
		for i, col := range cols {
			colval, ok := newRow.GetColVal(col.Tag)
			if !ok {
				return errors.New("error: could not get doc column value")
			}
			colVals[i] = colval
		}

		if len(colVals) < 2 {
			return errors.New("error: not enough values read from the table")
		}

		doc := Doc{}
		doc.DocPk = string(colVals[0].(types.String))
		doc.Text = []byte(colVals[1].(types.String))
		ret = append(ret, doc)

		return nil
	})

	return ret, err
}
