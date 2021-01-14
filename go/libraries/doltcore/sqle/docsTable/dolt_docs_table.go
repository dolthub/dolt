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

package docsTable

import (
	"context"
	"errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrDocsUpdate = errors.New("error updating local docs")
var ErrEmptyDocsTable = errors.New("error: All docs removed. Removing Docs Table")
var ErrMarshallingSchema = errors.New("error marshalling schema")

var doltDocsColumns, _ = schema.NewColCollection(
	schema.NewColumn(doltdb.DocPkColumnName, schema.DocNameTag, types.StringKind, true, schema.NotNullConstraint{}),
	schema.NewColumn(doltdb.DocTextColumnName, schema.DocTextTag, types.StringKind, false),
)
var DoltDocsSchema = schema.MustSchemaFromCols(doltDocsColumns)

// updateDocsTable takes in docTbl param and updates it with the value in docDetails. It returns the updated table.
func UpdateDocsTable(ctx context.Context, docTbl *doltdb.Table, docDetails []doltdb.DocDetails) (*doltdb.Table, error) {
	m, err := docTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := docTbl.GetSchema(context.Background())
	if err != nil {
		return nil, err
	}

	me := m.Edit()
	for _, doc := range docDetails {
		key, err := doltdb.DocTblKeyFromName(docTbl.Format(), doc.DocPk)
		if err != nil {
			return nil, err
		}

		docRow, exists, err := table.GetRow(ctx, docTbl, sch, key)
		if err != nil {
			return nil, err
		}

		if exists && doc.NewerText == nil {
			me = me.Remove(docRow.NomsMapKey(sch))
		} else if doc.NewerText != nil {
			docTaggedVals := row.TaggedValues{
				schema.DocNameTag: types.String(doc.DocPk),
				schema.DocTextTag: types.String(doc.NewerText),
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

// createDocTable creates a new in memory table that stores the given doc details.
func CreateDocsTable(ctx context.Context, vrw types.ValueReadWriter, docDetails []doltdb.DocDetails) (*doltdb.Table, error) {
	imt := table.NewInMemTable(DoltDocsSchema)

	// Determines if the table needs to be created at all and initializes a schema if it does.
	createTable := false
	for _, doc := range docDetails {
		if doc.NewerText != nil {
			createTable = true
			docTaggedVals := row.TaggedValues{
				schema.DocNameTag: types.String(doc.DocPk),
				schema.DocTextTag: types.String(doc.NewerText),
			}

			docRow, err := row.New(types.Format_7_18, DoltDocsSchema, docTaggedVals)
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
		wr := noms.NewNomsMapCreator(context.Background(), vrw, DoltDocsSchema)

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

		newDocsTbl, err := doltdb.NewTable(ctx, vrw, schVal, wr.GetMap(), empty)
		if err != nil {
			return nil, err
		}

		return newDocsTbl, nil
	}

	return nil, nil
}
