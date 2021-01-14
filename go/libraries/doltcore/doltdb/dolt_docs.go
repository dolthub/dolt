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

package doltdb

import (
	"context"
	"strconv"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type DocDetails struct {
	NewerText []byte
	DocPk     string
	Value     types.Value
	File      string
}

func DocTblKeyFromName(fmt *types.NomsBinFormat, name string) (types.Tuple, error) {
	return types.NewTuple(fmt, types.Uint(schema.DocNameTag), types.String(name))
}

func GetDocRow(ctx context.Context, docTbl *Table, sch schema.Schema, key types.Tuple) (r row.Row, ok bool, err error) {
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

// AddNewerTextToDocFromTbl updates the NewerText field of a docDetail using the provided table and schema.
func AddNewerTextToDocFromTbl(ctx context.Context, tbl *Table, sch *schema.Schema, doc DocDetails) (DocDetails, error) {
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
			doc.NewerText = []byte(docValue.(types.String))
		} else {
			doc.NewerText = nil
		}
	} else {
		doc.NewerText = nil
	}
	return doc, nil
}

func addNewerTextToDocFromRow(ctx context.Context, r row.Row, doc *DocDetails) (DocDetails, error) {
	docValue, ok := r.GetColVal(schema.DocTextTag)
	if !ok {
		doc.NewerText = nil
	} else {
		docValStr, err := strconv.Unquote(docValue.HumanReadableString())
		if err != nil {
			return DocDetails{}, err
		}
		doc.NewerText = []byte(docValStr)
	}
	return *doc, nil
}

func addDocPKToDocFromRow(r row.Row, doc *DocDetails) (DocDetails, error) {
	colVal, _ := r.GetColVal(schema.DocNameTag)
	if colVal == nil {
		doc.DocPk = ""
	} else {
		docName, err := strconv.Unquote(colVal.HumanReadableString())
		if err != nil {
			return DocDetails{}, err
		}
		doc.DocPk = docName
	}

	return *doc, nil
}
