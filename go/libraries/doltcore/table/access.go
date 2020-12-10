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

package table

import (
	"context"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

// GetRow returns a row from |tbl| corresponding to |key| if it exists.
func GetRow(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, key types.Tuple) (r row.Row, ok bool, err error) {
	rowMap, err := tbl.GetRowData(ctx)
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

// ForeignKeyIsSatisfied ensures that the foreign key is valid by comparing the index data from the given table
// against the index data from the referenced table.
func ForeignKeyIsSatisfied(ctx context.Context, fk doltdb.ForeignKey, childIdx, parentIdx types.Map, childDef, parentDef schema.Index) error {
	if fk.ReferencedTableIndex != parentDef.Name() {
		return fmt.Errorf("cannot validate data as wrong referenced index was given: expected `%s` but received `%s`",
			fk.ReferencedTableIndex, parentDef.Name())
	}

	tagMap := make(map[uint64]uint64, len(fk.TableColumns))
	for i, childTag := range fk.TableColumns {
		tagMap[childTag] = fk.ReferencedTableColumns[i]
	}

	// FieldMappings ignore columns not in the tagMap
	fm, err := rowconv.NewFieldMapping(childDef.Schema(), parentDef.Schema(), tagMap)
	if err != nil {
		return err
	}

	rc, err := rowconv.NewRowConverter(fm)
	if err != nil {
		return err
	}

	rdr, err := noms.NewNomsMapReader(ctx, childIdx, childDef.Schema())
	if err != nil {
		return err
	}

	for {
		childIdxRow, err := rdr.ReadRow(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		parentIdxRow, err := rc.Convert(childIdxRow)
		if err != nil {
			return err
		}
		if row.IsEmpty(parentIdxRow) {
			continue
		}

		partial, err := parentIdxRow.ReduceToIndexPartialKey(parentDef)
		if err != nil {
			return err
		}

		indexIter := noms.NewNomsRangeReader(parentDef.Schema(), parentIdx,
			[]*noms.ReadRange{{Start: partial, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
				return tuple.StartsWith(partial), nil
			}}},
		)

		switch _, err = indexIter.ReadRow(ctx); err {
		case nil:
			continue // parent table contains child key
		case io.EOF:
			indexKeyStr, _ := types.EncodedValue(ctx, partial)
			return fmt.Errorf("foreign key violation on `%s`.`%s`: `%s`", fk.Name, fk.TableName, indexKeyStr)
		default:
			return err
		}
	}

	return nil
}
