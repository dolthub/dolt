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
	"strconv"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// updateDocsTable takes in docTbl param and updates it with the value in docs. It returns the updated table.
func updateDocsTable(ctx context.Context, docTbl *doltdb.Table, docs Docs) (*doltdb.Table, error) {
	return nil, nil
}

// createDocsTable creates a new in memory table that stores the given doc details.
func createDocsTable(ctx context.Context, vrw types.ValueReadWriter, docs Docs) (*doltdb.Table, error) {
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
	return
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
	return nil, nil
}
