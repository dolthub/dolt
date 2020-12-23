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

package editor

import (
	"context"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

type editDependency interface {
	insertRow(ctx context.Context, r row.Row) error
	updateRow(ctx context.Context, old, new row.Row) error
	deleteRow(ctx context.Context, r row.Row) error
}

// ContainsIndexedKey returns whether the given key is contained within the index. The key is assumed to be in the
// format expected of the index, similar to searching on the index map itself.
func ContainsIndexedKey(ctx context.Context, te TableEditor, key types.Tuple, indexName string) (bool, error) {
	tbl, err := te.Table(ctx)
	if err != nil {
		return false, err
	}

	idxSch := te.Schema().Indexes().GetByName(indexName)
	idxMap, err := tbl.GetIndexRowData(ctx, indexName)
	if err != nil {
		return false, err
	}

	indexIter := noms.NewNomsRangeReader(idxSch.Schema(), idxMap,
		[]*noms.ReadRange{{Start: key, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
			return tuple.StartsWith(key), nil
		}}},
	)

	_, err = indexIter.ReadRow(ctx)
	if err == nil { // row exists
		return true, nil
	} else if err != io.EOF {
		return false, err
	} else {
		return false, nil
	}
}

// GetIndexedRows returns all matching rows for the given key on the index. The key is assumed to be in the format
// expected of the index, similar to searching on the index map itself.
func GetIndexedRows(ctx context.Context, te TableEditor, key types.Tuple, indexName string) ([]row.Row, error) {
	tbl, err := te.Table(ctx)
	if err != nil {
		return nil, err
	}

	idxSch := te.Schema().Indexes().GetByName(indexName)
	idxMap, err := tbl.GetIndexRowData(ctx, indexName)
	if err != nil {
		return nil, err
	}

	indexIter := noms.NewNomsRangeReader(idxSch.Schema(), idxMap,
		[]*noms.ReadRange{{Start: key, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
			return tuple.StartsWith(key), nil
		}}},
	)

	rowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	var rows []row.Row
	for {
		r, err := indexIter.ReadRow(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		indexRowTaggedValues, err := row.GetTaggedVals(r)
		if err != nil {
			return nil, err
		}

		pkTuple := indexRowTaggedValues.NomsTupleForPKCols(te.Format(), te.Schema().GetPKCols())
		pkTupleVal, err := pkTuple.Value(ctx)
		if err != nil {
			return nil, err
		}

		fieldsVal, _, err := rowData.MaybeGet(ctx, pkTupleVal)
		if err != nil {
			return nil, err
		}
		if fieldsVal == nil {
			keyStr, err := formatKey(ctx, key)
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("index key `%s` does not have a corresponding entry in table", keyStr)
		}

		tableRow, err := row.FromNoms(te.Schema(), pkTupleVal.(types.Tuple), fieldsVal.(types.Tuple))
		rows = append(rows, tableRow)
	}

	return rows, nil
}

// reduceRowAndConvert takes in a row and returns a Tuple containing only the values from the tags given. The returned
// items have tags from newTags, while the tags from dRow are expected to match originalTags. Both parameter slices are
// assumed to have equivalent ordering and length. If the key contains any nulls, then we return true to indicate that
// we do not propagate an ON DELETE/UPDATE.
func reduceRowAndConvert(nbf *types.NomsBinFormat, originalTags []uint64, newTags []uint64, dRow row.Row) (types.Tuple, bool, error) {
	keyVals := make([]types.Value, len(originalTags)*2)
	for i, colTag := range originalTags {
		val, ok := dRow.GetColVal(colTag)
		if !ok {
			return types.EmptyTuple(nbf), true, nil
		}
		newTag := newTags[i]
		keyVals[2*i] = types.Uint(newTag)
		keyVals[2*i+1] = val
	}
	tpl, err := types.NewTuple(nbf, keyVals...)
	if err != nil {
		return types.EmptyTuple(nbf), false, err
	}
	return tpl, false, nil
}

func newChildDependency(ste *sessionedTableEditor, fk doltdb.ForeignKey) editDependency {
	return fkChildDependency{sessionedTableEditor: ste, fk: fk}
}

type fkChildDependency struct {
	*sessionedTableEditor
	fk doltdb.ForeignKey
}

func (dep fkChildDependency) insertRow(_ context.Context, _ row.Row) error {
	return nil
}

func (dep fkChildDependency) updateRow(ctx context.Context, old, new row.Row) error {
	indexKey, hasNulls, err := reduceRowAndConvert(dep.Format(), dep.fk.ReferencedTableColumns, dep.fk.TableColumns, old)
	if err != nil {
		return err
	}
	if hasNulls {
		// todo: this seems wrong
		return nil
	}
	referencingRows, err := GetIndexedRows(ctx, dep.tableEditor, indexKey, dep.fk.TableIndex)
	if err != nil {
		return err
	}
	if len(referencingRows) == 0 {
		return nil
	}

	valueChanged := false
	for _, colTag := range dep.fk.ReferencedTableColumns {
		oldVal, oldOk := old.GetColVal(colTag)
		newVal, newOk := new.GetColVal(colTag)
		if (oldOk != newOk) || (oldOk && newOk && !oldVal.Equals(newVal)) {
			valueChanged = true
			break
		}
	}
	if !valueChanged {
		return nil
	}

	switch dep.fk.OnUpdate {
	case doltdb.ForeignKeyReferenceOption_Cascade:
		// NULL handling is usually done higher, so if a new value is NULL then we need to error
		for i := range dep.fk.ReferencedTableColumns {
			if incomingVal, _ := new.GetColVal(dep.fk.ReferencedTableColumns[i]); types.IsNull(incomingVal) {
				col, ok := dep.Schema().GetAllCols().GetByTag(dep.fk.TableColumns[i])
				if !ok {
					return fmt.Errorf("column with tag `%d` not found on `%s` from foreign key `%s`",
						dep.fk.TableColumns[i], dep.fk.TableName, dep.fk.Name)
				}
				if !col.IsNullable() {
					return fmt.Errorf("column name `%s`.`%s` is non-nullable but attempted to set a value of NULL from foreign key `%s`",
						dep.fk.TableName, col.Name, dep.fk.Name)
				}
			}
		}
		for _, rowToUpdate := range referencingRows {
			newRow := rowToUpdate
			for i := range dep.fk.ReferencedTableColumns {
				incomingVal, _ := new.GetColVal(dep.fk.ReferencedTableColumns[i])
				newRow, err = newRow.SetColVal(dep.fk.TableColumns[i], incomingVal, dep.Schema())
				if err != nil {
					return err
				}
			}
			err = dep.sessionedTableEditor.updateRow(ctx, rowToUpdate, newRow, false)
			if err != nil {
				return err
			}
		}
	case doltdb.ForeignKeyReferenceOption_SetNull:
		for _, oldRow := range referencingRows {
			newRow := oldRow
			for _, colTag := range dep.fk.TableColumns {
				newRow, err = newRow.SetColVal(colTag, types.NullValue, dep.Schema())
				if err != nil {
					return err
				}
			}
			err = dep.sessionedTableEditor.updateRow(ctx, oldRow, newRow, false)
			if err != nil {
				return err
			}
		}
	case doltdb.ForeignKeyReferenceOption_DefaultAction, doltdb.ForeignKeyReferenceOption_NoAction, doltdb.ForeignKeyReferenceOption_Restrict:
		indexKeyStr, _ := types.EncodedValue(ctx, indexKey)
		return fmt.Errorf("foreign key constraint violation on `%s`.`%s`: cannot update rows with value `%s`",
			dep.fk.TableName, dep.fk.Name, indexKeyStr)
	default:
		return fmt.Errorf("unknown ON UPDATE reference option on `%s`: `%s`", dep.fk.Name, dep.fk.OnUpdate.String())
	}

	return nil
}

func (dep fkChildDependency) deleteRow(ctx context.Context, dRow row.Row) error {
	indexKey, hasNulls, err := reduceRowAndConvert(dep.Format(), dep.fk.ReferencedTableColumns, dep.fk.TableColumns, dRow)
	if err != nil {
		return err
	}
	if hasNulls {
		// todo: this seems wrong
		return nil
	}
	referencingRows, err := GetIndexedRows(ctx, dep.tableEditor, indexKey, dep.fk.TableIndex)
	if err != nil {
		return err
	}
	if len(referencingRows) == 0 {
		return nil
	}

	switch dep.fk.OnDelete {
	case doltdb.ForeignKeyReferenceOption_Cascade:
		for _, rowToDelete := range referencingRows {
			err = dep.sessionedTableEditor.DeleteRow(ctx, rowToDelete)
			if err != nil {
				return err
			}
		}
	case doltdb.ForeignKeyReferenceOption_SetNull:
		for _, oldRow := range referencingRows {
			newRow := oldRow
			for _, colTag := range dep.fk.TableColumns {
				newRow, err = newRow.SetColVal(colTag, types.NullValue, dep.Schema())
				if err != nil {
					return err
				}
			}
			err = dep.sessionedTableEditor.updateRow(ctx, oldRow, newRow, false)
			if err != nil {
				return err
			}
		}
	case doltdb.ForeignKeyReferenceOption_DefaultAction, doltdb.ForeignKeyReferenceOption_NoAction, doltdb.ForeignKeyReferenceOption_Restrict:
		indexKeyStr, _ := types.EncodedValue(ctx, indexKey)
		return fmt.Errorf("foreign key constraint violation on `%s`.`%s`: cannot delete rows with value `%s`",
			dep.fk.TableName, dep.fk.Name, indexKeyStr)
	default:
		return fmt.Errorf("unknown ON DELETE reference option on `%s`: `%s`", dep.fk.Name, dep.fk.OnDelete.String())
	}
	return nil
}

func newParentDependency(ste *sessionedTableEditor, fk doltdb.ForeignKey) editDependency {
	return fkParentDependency{sessionedTableEditor: ste, fk: fk}
}

type fkParentDependency struct {
	*sessionedTableEditor
	fk doltdb.ForeignKey
}

func (dep fkParentDependency) insertRow(ctx context.Context, dRow row.Row) error {
	indexKey, hasNulls, err := reduceRowAndConvert(dep.Format(), dep.fk.TableColumns, dep.fk.ReferencedTableColumns, dRow)
	if err != nil {
		return err
	}
	if hasNulls {
		// todo: seems wrong
		return nil
	}
	exists, err := ContainsIndexedKey(ctx, dep.tableEditor, indexKey, dep.fk.ReferencedTableIndex)
	if err != nil {
		return err
	}
	if !exists {
		indexKeyStr, _ := types.EncodedValue(ctx, indexKey)
		return fmt.Errorf("foreign key violation on `%s`.`%s`: `%s`", dep.fk.TableName, dep.fk.Name, indexKeyStr)
	}

	return nil
}

func (dep fkParentDependency) updateRow(ctx context.Context, _, new row.Row) error {
	return dep.insertRow(ctx, new)
}

func (dep fkParentDependency) deleteRow(_ context.Context, _ row.Row) error {
	return nil
}
