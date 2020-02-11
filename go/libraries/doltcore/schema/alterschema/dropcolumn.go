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

package alterschema

import (
	"context"
	"errors"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// DropColumn drops a column from a table, and removes its associated cell values
func DropColumn(ctx context.Context, tbl *doltdb.Table, colName string) (*doltdb.Table, error) {
	if tbl == nil {
		panic("invalid parameters")
	}

	tblSch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	allCols := tblSch.GetAllCols()

	var dropTag uint64
	if col, ok := allCols.GetByName(colName); !ok {
		return nil, schema.ErrColNotFound
	} else if col.IsPartOfPK {
		return nil, errors.New("Cannot drop column in primary key")
	} else {
		dropTag = col.Tag
	}

	cols := make([]schema.Column, 0)
	err = allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Name != colName {
			cols = append(cols, col)
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		return nil, err
	}

	newSch := schema.SchemaFromCols(colColl)

	vrw := tbl.ValueReadWriter()
	schemaVal, err := encoding.MarshalAsNomsValue(ctx, vrw, newSch)

	if err != nil {
		return nil, err
	}

	rd, err := tbl.GetRowData(ctx)

	prunedRowData, err := dropColumnValuesForTag(ctx, tbl.Format(), newSch, rd, dropTag)

	if err != nil {
		return nil, err
	}

	newTable, err := doltdb.NewTable(ctx, vrw, schemaVal, prunedRowData)

	if err != nil {
		return nil, err
	}

	return newTable, nil
}

func dropColumnValuesForTag(ctx context.Context, nbf *types.NomsBinFormat, newSch schema.Schema, rowData types.Map, dropTag uint64) (types.Map, error) {
	re := rowData.Edit()

	mi, err := rowData.BufferedIterator(ctx)

	if err != nil {
		return types.EmptyMap, err
	}

	for {
		k, v, err := mi.Next(ctx)

		if k == nil || v == nil {
			break
		}

		if err != nil {
			return types.EmptyMap, err
		}

		// can't drop primary key columns, tag is in map value
		tv, err := row.ParseTaggedValues(v.(types.Tuple))

		if err != nil {
			return types.EmptyMap, err
		}

		delete(tv, dropTag)

		re.Set(k, tv.NomsTupleForTags(nbf, newSch.GetNonPKCols().Tags, false))
	}

	prunedRowData, err := re.Map(ctx)

	if err != nil {
		return types.EmptyMap, nil
	}

	return prunedRowData, nil
}
