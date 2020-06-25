// Copyright 2020 Liquidata, Inc.
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
	"fmt"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// SessionedTableEditor represents a table editor obtained from a TableEditSession. This table editor may be shared
// by multiple callers. It is thread safe.
type SessionedTableEditor struct {
	tableEditSession  *TableEditSession
	tableEditor       *TableEditor
	referencedTables  []*ForeignKey
	referencingTables []*ForeignKey
}

// ContainsIndexedKey returns whether the given key is contained within the index. The key is assumed to be in the
// format expected of the index, similar to searching on the index map itself.
func (ste *SessionedTableEditor) ContainsIndexedKey(ctx context.Context, key types.Tuple, indexName string) (bool, error) {
	return ste.tableEditor.ContainsIndexedKey(ctx, key, indexName)
}

// GetIndexedRows returns all matching rows for the given key on the index. The key is assumed to be in the format
// expected of the index, similar to searching on the index map itself.
func (ste *SessionedTableEditor) GetIndexedRows(ctx context.Context, key types.Tuple, indexName string) ([]row.Row, error) {
	return ste.tableEditor.GetIndexedRows(ctx, key, indexName)
}

// GetRow returns the row matching the key given from the TableEditor. This is equivalent to calling Table and then
// GetRow on the returned table, but a tad faster.
func (ste *SessionedTableEditor) GetRow(ctx context.Context, key types.Tuple) (row.Row, bool, error) {
	return ste.tableEditor.GetRow(ctx, key)
}

// GetRoot is a shortcut to calling SessionTableEditor.GetRoot.
func (ste *SessionedTableEditor) GetRoot(ctx context.Context) (*RootValue, error) {
	return ste.tableEditSession.GetRoot(ctx)
}

// InsertRow adds the given row to the table. If the row already exists, use UpdateRow.
func (ste *SessionedTableEditor) InsertRow(ctx context.Context, dRow row.Row) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	err := ste.validateForInsert(ctx, dRow)
	if err != nil {
		return err
	}

	return ste.tableEditor.InsertRow(ctx, dRow)
}

// DeleteKey removes the given key from the table.
func (ste *SessionedTableEditor) DeleteKey(ctx context.Context, key types.Tuple) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	if !ste.tableEditSession.Props.ForeignKeyChecksDisabled && len(ste.referencingTables) > 0 {
		dRow, ok, err := ste.tableEditor.GetRow(ctx, key)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		err = ste.handleReferencingRowsOnDelete(ctx, dRow)
		if err != nil {
			return err
		}
	}

	return ste.tableEditor.DeleteKey(ctx, key)
}

// DeleteRow removes the given row from the table, along with any applicable rows from tables that have a foreign key
// referencing this table.
func (ste *SessionedTableEditor) DeleteRow(ctx context.Context, dRow row.Row) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	return ste.deleteRow(ctx, dRow)
}

// UpdateRow takes the current row and new row, and updates it accordingly. Any applicable rows from tables that have a
// foreign key referencing this table will also be updated.
func (ste *SessionedTableEditor) UpdateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	return ste.updateRow(ctx, dOldRow, dNewRow, true)
}

// deleteRow is the same as DeleteRow, except that it does not acquire any locks.
func (ste *SessionedTableEditor) deleteRow(ctx context.Context, dRow row.Row) error {
	err := ste.handleReferencingRowsOnDelete(ctx, dRow)
	if err != nil {
		return err
	}

	return ste.tableEditor.DeleteRow(ctx, dRow)
}

// handleReferencingRowsOnDelete handles updating referencing foreign keys on delete operations
func (ste *SessionedTableEditor) handleReferencingRowsOnDelete(ctx context.Context, dRow row.Row) error {
	if ste.tableEditSession.Props.ForeignKeyChecksDisabled {
		return nil
	}

	for _, foreignKey := range ste.referencingTables {
		var err error
		referencingSte, err := ste.tableEditSession.getTableEditorOrFail(foreignKey.TableName)
		if err != nil {
			return err
		}
		indexKey, err := ste.reduceRowAndConvert(ste.tableEditor.nbf, foreignKey.ReferencedTableColumns, foreignKey.TableColumns, dRow)
		if err != nil {
			return err
		}
		referencingRows, err := referencingSte.tableEditor.GetIndexedRows(ctx, indexKey, foreignKey.TableIndex)
		if err != nil {
			return err
		}
		if len(referencingRows) == 0 {
			continue
		}

		switch foreignKey.OnDelete {
		case ForeignKeyReferenceOption_Cascade:
			for _, rowToDelete := range referencingRows {
				err = referencingSte.DeleteRow(ctx, rowToDelete)
				if err != nil {
					return err
				}
			}
		case ForeignKeyReferenceOption_SetNull:
			for _, oldRow := range referencingRows {
				newRow := oldRow
				for _, colTag := range foreignKey.TableColumns {
					newRow, err = newRow.SetColVal(colTag, types.NullValue, referencingSte.tableEditor.tSch)
					if err != nil {
						return err
					}
				}
				err = referencingSte.updateRow(ctx, oldRow, newRow, false)
				if err != nil {
					return err
				}
			}
		case ForeignKeyReferenceOption_DefaultAction, ForeignKeyReferenceOption_NoAction, ForeignKeyReferenceOption_Restrict:
			indexKeyStr, _ := types.EncodedValue(ctx, indexKey)
			return fmt.Errorf("foreign key constraint violation on `%s`.`%s`: cannot delete rows with value `%s`",
				foreignKey.TableName, foreignKey.Name, indexKeyStr)
		default:
			return fmt.Errorf("unknown ON DELETE reference option on `%s`: `%s`", foreignKey.Name, foreignKey.OnDelete.String())
		}
	}

	return nil
}

func (ste *SessionedTableEditor) handleReferencingRowsOnUpdate(ctx context.Context, dOldRow row.Row, dNewRow row.Row) error {
	if ste.tableEditSession.Props.ForeignKeyChecksDisabled {
		return nil
	}

	for _, foreignKey := range ste.referencingTables {
		var err error
		referencedSte, err := ste.tableEditSession.getTableEditorOrFail(foreignKey.TableName)
		if err != nil {
			return err
		}
		indexKey, err := ste.reduceRowAndConvert(ste.tableEditor.nbf, foreignKey.ReferencedTableColumns, foreignKey.TableColumns, dOldRow)
		if err != nil {
			return err
		}
		referencingRows, err := referencedSte.tableEditor.GetIndexedRows(ctx, indexKey, foreignKey.TableIndex)
		if err != nil {
			return err
		}
		if len(referencingRows) == 0 {
			continue
		}

		valueChanged := false
		for _, colTag := range foreignKey.ReferencedTableColumns {
			oldVal, oldOk := dOldRow.GetColVal(colTag)
			newVal, newOk := dNewRow.GetColVal(colTag)
			if (oldOk != newOk) || (oldOk && newOk && !oldVal.Equals(newVal)) {
				valueChanged = true
				break
			}
		}
		if !valueChanged {
			continue
		}

		switch foreignKey.OnUpdate {
		case ForeignKeyReferenceOption_Cascade:
			// NULL handling is usually done higher, so if a new value is NULL then we need to error
			for i := range foreignKey.ReferencedTableColumns {
				if incomingVal, _ := dNewRow.GetColVal(foreignKey.ReferencedTableColumns[i]); types.IsNull(incomingVal) {
					col, ok := referencedSte.tableEditor.tSch.GetAllCols().GetByTag(foreignKey.TableColumns[i])
					if !ok {
						return fmt.Errorf("column with tag `%d` not found on `%s` from foreign key `%s`",
							foreignKey.TableColumns[i], foreignKey.TableName, foreignKey.Name)
					}
					if !col.IsNullable() {
						return fmt.Errorf("column name `%s`.`%s` is non-nullable but attempted to set a value of NULL from foreign key `%s`",
							foreignKey.TableName, col.Name, foreignKey.Name)
					}
				}
			}
			for _, rowToUpdate := range referencingRows {
				newRow := rowToUpdate
				for i := range foreignKey.ReferencedTableColumns {
					incomingVal, _ := dNewRow.GetColVal(foreignKey.ReferencedTableColumns[i])
					newRow, err = newRow.SetColVal(foreignKey.TableColumns[i], incomingVal, referencedSte.tableEditor.tSch)
					if err != nil {
						return err
					}
				}
				err = referencedSte.updateRow(ctx, rowToUpdate, newRow, false)
				if err != nil {
					return err
				}
			}
		case ForeignKeyReferenceOption_SetNull:
			for _, oldRow := range referencingRows {
				newRow := oldRow
				for _, colTag := range foreignKey.TableColumns {
					newRow, err = newRow.SetColVal(colTag, types.NullValue, referencedSte.tableEditor.tSch)
					if err != nil {
						return err
					}
				}
				err = referencedSte.updateRow(ctx, oldRow, newRow, false)
				if err != nil {
					return err
				}
			}
		case ForeignKeyReferenceOption_DefaultAction, ForeignKeyReferenceOption_NoAction, ForeignKeyReferenceOption_Restrict:
			indexKeyStr, _ := types.EncodedValue(ctx, indexKey)
			return fmt.Errorf("foreign key constraint violation on `%s`.`%s`: cannot update rows with value `%s`",
				foreignKey.TableName, foreignKey.Name, indexKeyStr)
		default:
			return fmt.Errorf("unknown ON UPDATE reference option on `%s`: `%s`", foreignKey.Name, foreignKey.OnUpdate.String())
		}
	}

	return nil
}

// reduceRowAndConvert takes in a row and returns a Tuple containing only the values from the tags given. The returned
// items have tags from newTags, while the tags from dRow are expected to match originalTags. Both parameter slices are
// assumed to have equivalent ordering and length.
func (ste *SessionedTableEditor) reduceRowAndConvert(nbf *types.NomsBinFormat, originalTags []uint64, newTags []uint64, dRow row.Row) (types.Tuple, error) {
	keyVals := make([]types.Value, len(originalTags)*2)
	for i, colTag := range originalTags {
		val, ok := dRow.GetColVal(colTag)
		if !ok {
			val = types.NullValue
		}
		newTag := newTags[i]
		keyVals[2*i] = types.Uint(newTag)
		keyVals[2*i+1] = val
	}
	tpl, err := types.NewTuple(nbf, keyVals...)
	if err != nil {
		return types.EmptyTuple(nbf), err
	}
	return tpl, nil
}

func (ste *SessionedTableEditor) updateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row, checkReferences bool) error {
	if checkReferences {
		err := ste.validateForInsert(ctx, dNewRow)
		if err != nil {
			return err
		}
	}

	err := ste.handleReferencingRowsOnUpdate(ctx, dOldRow, dNewRow)
	if err != nil {
		return err
	}

	return ste.tableEditor.UpdateRow(ctx, dOldRow, dNewRow)
}

// validateForInsert returns whether the given row is able to be inserted into the target table.
func (ste *SessionedTableEditor) validateForInsert(ctx context.Context, dRow row.Row) error {
	if ste.tableEditSession.Props.ForeignKeyChecksDisabled {
		return nil
	}
	for _, foreignKey := range ste.referencedTables {
		// first check if it's all nulls, as they are always valid to INSERT
		allNulls := true
		for _, colTag := range foreignKey.TableColumns {
			val, ok := dRow.GetColVal(colTag)
			if ok && !types.IsNull(val) {
				allNulls = false
			}
		}
		if allNulls {
			return nil
		}

		indexKey, err := ste.reduceRowAndConvert(ste.tableEditor.nbf, foreignKey.TableColumns, foreignKey.ReferencedTableColumns, dRow)
		if err != nil {
			return err
		}
		referencedSte, err := ste.tableEditSession.getTableEditorOrFail(foreignKey.ReferencedTableName)
		if err != nil {
			return err
		}
		exists, err := referencedSte.tableEditor.ContainsIndexedKey(ctx, indexKey, foreignKey.ReferencedTableIndex)
		if err != nil {
			return err
		}
		if !exists {
			indexKeyStr, _ := types.EncodedValue(ctx, indexKey)
			return fmt.Errorf("foreign key violation on `%s`.`%s`: `%s`", foreignKey.TableName, foreignKey.Name, indexKeyStr)
		}
	}
	return nil
}
