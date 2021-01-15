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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type DocDetails struct {
	Text  []byte
	DocPk string
	File  string
}

func DocTblKeyFromName(fmt *types.NomsBinFormat, name string) (types.Tuple, error) {
	return types.NewTuple(fmt, types.Uint(schema.DocNameTag), types.String(name))
}

func GetDocRow(ctx context.Context, docTbl *doltdb.Table, sch schema.Schema, key types.Tuple) (r row.Row, ok bool, err error) {
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
	return
}

// AddNewerTextToDocFromTbl updates the Text field of a docDetail using the provided table and schema.
func AddNewerTextToDocFromTbl(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, doc DocDetails) (DocDetails, error) {
	if tbl != nil && sch != nil {
		key, err := DocTblKeyFromName(tbl.Format(), doc.DocPk)
		if err != nil {
			return DocDetails{}, err
		}

		docRow, ok, err := GetDocRow(ctx, tbl, *sch, key)
		if err != nil {
			return DocDetails{}, err
		}
		if ok {
			docValue, _ := docRow.GetColVal(schema.DocTextTag)
			doc.Text = []byte(docValue.(types.String))
		} else {
			doc.Text = nil
		}
	} else {
		doc.Text = nil
	}
	return doc, nil
}