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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// sessionedTableEditor represents a table editor obtained from a TableEditSession. This table editor may be shared
// by multiple callers. It is thread safe.
type sessionedTableEditor struct {
	tableEditSession  *TableEditSession
	tableEditor       TableEditor
	referencedTables  []doltdb.ForeignKey // The tables that we reference to ensure an insert or update is valid
	referencingTables []doltdb.ForeignKey // The tables that reference us to ensure their inserts and updates are valid
}

var _ TableEditor = &sessionedTableEditor{}

// InsertRow adds the given row to the table. If the row already exists, use UpdateRow.
func (ste *sessionedTableEditor) InsertRow(ctx context.Context, dRow row.Row) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	err := ste.validateForInsert(ctx, dRow)
	if err != nil {
		return err
	}

	return ste.tableEditor.InsertRow(ctx, dRow)
}

// DeleteKey removes the given key from the table.
func (ste *sessionedTableEditor) DeleteRow(ctx context.Context, r row.Row) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	if !ste.tableEditSession.Props.ForeignKeyChecksDisabled && len(ste.referencingTables) > 0 {
		err := ste.handleReferencingRowsOnDelete(ctx, r)
		if err != nil {
			return err
		}
	}

	return ste.tableEditor.DeleteRow(ctx, r)
}

// UpdateRow takes the current row and new row, and updates it accordingly. Any applicable rows from tables that have a
// foreign key referencing this table will also be updated.
func (ste *sessionedTableEditor) UpdateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	return ste.updateRow(ctx, dOldRow, dNewRow, true)
}

func (ste *sessionedTableEditor) GetAutoIncrementValue() types.Value {
	return ste.tableEditor.GetAutoIncrementValue()
}

func (ste *sessionedTableEditor) SetAutoIncrementValue(v types.Value) error {
	return ste.tableEditor.SetAutoIncrementValue(v)
}

func (ste *sessionedTableEditor) Table(ctx context.Context) (*doltdb.Table, error) {
	root, err := ste.tableEditSession.Flush(ctx)
	if err != nil {
		return nil, err
	}

	name := ste.tableEditor.Name()
	tbl, ok, err := root.GetTable(ctx, name)
	if !ok {
		return nil, fmt.Errorf("edit session failed to flush table %s", name)
	}
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

func (ste *sessionedTableEditor) Schema() schema.Schema {
	return ste.tableEditor.Schema()
}

func (ste *sessionedTableEditor) Name() string {
	return ste.tableEditor.Name()
}

func (ste *sessionedTableEditor) Format() *types.NomsBinFormat {
	return ste.tableEditor.Format()
}

func (ste *sessionedTableEditor) Close() error {
	return ste.tableEditor.Close()
}

// handleReferencingRowsOnDelete handles updating referencing foreign keys on delete operations
func (ste *sessionedTableEditor) handleReferencingRowsOnDelete(ctx context.Context, dRow row.Row) error {
	if ste.tableEditSession.Props.ForeignKeyChecksDisabled {
		return nil
	}

	for _, foreignKey := range ste.referencingTables {
		referencingSte, ok := ste.tableEditSession.tables[foreignKey.TableName]
		if !ok {
			return fmt.Errorf("unable to get table editor as `%s` is missing", foreignKey.TableName)
		}
		indexKey, hasNulls, err := ste.reduceRowAndConvert(ste.tableEditor.Format(), foreignKey.ReferencedTableColumns, foreignKey.TableColumns, dRow)
		if err != nil {
			return err
		}
		if hasNulls {
			continue
		}
		referencingRows, err := GetIndexedRows(ctx, referencingSte.tableEditor, indexKey, foreignKey.TableIndex)
		if err != nil {
			return err
		}
		if len(referencingRows) == 0 {
			continue
		}

		switch foreignKey.OnDelete {
		case doltdb.ForeignKeyReferenceOption_Cascade:
			for _, rowToDelete := range referencingRows {
				err = referencingSte.DeleteRow(ctx, rowToDelete)
				if err != nil {
					return err
				}
			}
		case doltdb.ForeignKeyReferenceOption_SetNull:
			for _, oldRow := range referencingRows {
				newRow := oldRow
				for _, colTag := range foreignKey.TableColumns {
					newRow, err = newRow.SetColVal(colTag, types.NullValue, referencingSte.tableEditor.Schema())
					if err != nil {
						return err
					}
				}
				err = referencingSte.updateRow(ctx, oldRow, newRow, false)
				if err != nil {
					return err
				}
			}
		case doltdb.ForeignKeyReferenceOption_DefaultAction, doltdb.ForeignKeyReferenceOption_NoAction, doltdb.ForeignKeyReferenceOption_Restrict:
			indexKeyStr, _ := types.EncodedValue(ctx, indexKey)
			return fmt.Errorf("foreign key constraint violation on `%s`.`%s`: cannot delete rows with value `%s`",
				foreignKey.TableName, foreignKey.Name, indexKeyStr)
		default:
			return fmt.Errorf("unknown ON DELETE reference option on `%s`: `%s`", foreignKey.Name, foreignKey.OnDelete.String())
		}
	}

	return nil
}

func (ste *sessionedTableEditor) handleReferencingRowsOnUpdate(ctx context.Context, dOldRow row.Row, dNewRow row.Row) error {
	if ste.tableEditSession.Props.ForeignKeyChecksDisabled {
		return nil
	}

	for _, foreignKey := range ste.referencingTables {
		referencingSte, ok := ste.tableEditSession.tables[foreignKey.TableName]
		if !ok {
			return fmt.Errorf("unable to get table editor as `%s` is missing", foreignKey.TableName)
		}
		indexKey, hasNulls, err := ste.reduceRowAndConvert(ste.tableEditor.Format(), foreignKey.ReferencedTableColumns, foreignKey.TableColumns, dOldRow)
		if err != nil {
			return err
		}
		if hasNulls {
			continue
		}
		referencingRows, err := GetIndexedRows(ctx, referencingSte.tableEditor, indexKey, foreignKey.TableIndex)
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
		case doltdb.ForeignKeyReferenceOption_Cascade:
			// NULL handling is usually done higher, so if a new value is NULL then we need to error
			for i := range foreignKey.ReferencedTableColumns {
				if incomingVal, _ := dNewRow.GetColVal(foreignKey.ReferencedTableColumns[i]); types.IsNull(incomingVal) {
					col, ok := referencingSte.tableEditor.Schema().GetAllCols().GetByTag(foreignKey.TableColumns[i])
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
					newRow, err = newRow.SetColVal(foreignKey.TableColumns[i], incomingVal, referencingSte.tableEditor.Schema())
					if err != nil {
						return err
					}
				}
				err = referencingSte.updateRow(ctx, rowToUpdate, newRow, false)
				if err != nil {
					return err
				}
			}
		case doltdb.ForeignKeyReferenceOption_SetNull:
			for _, oldRow := range referencingRows {
				newRow := oldRow
				for _, colTag := range foreignKey.TableColumns {
					newRow, err = newRow.SetColVal(colTag, types.NullValue, referencingSte.tableEditor.Schema())
					if err != nil {
						return err
					}
				}
				err = referencingSte.updateRow(ctx, oldRow, newRow, false)
				if err != nil {
					return err
				}
			}
		case doltdb.ForeignKeyReferenceOption_DefaultAction, doltdb.ForeignKeyReferenceOption_NoAction, doltdb.ForeignKeyReferenceOption_Restrict:
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
// assumed to have equivalent ordering and length. If the key contains any nulls, then we return true to indicate that
// we do not propagate an ON DELETE/UPDATE.
func (ste *sessionedTableEditor) reduceRowAndConvert(nbf *types.NomsBinFormat, originalTags []uint64, newTags []uint64, dRow row.Row) (types.Tuple, bool, error) {
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

func (ste *sessionedTableEditor) updateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row, checkReferences bool) error {
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
func (ste *sessionedTableEditor) validateForInsert(ctx context.Context, dRow row.Row) error {
	if ste.tableEditSession.Props.ForeignKeyChecksDisabled {
		return nil
	}
	for _, foreignKey := range ste.referencedTables {
		indexKey, hasNulls, err := ste.reduceRowAndConvert(ste.tableEditor.Format(), foreignKey.TableColumns, foreignKey.ReferencedTableColumns, dRow)
		if err != nil {
			return err
		}
		if hasNulls {
			continue
		}
		referencingSte, ok := ste.tableEditSession.tables[foreignKey.ReferencedTableName]
		if !ok {
			return fmt.Errorf("unable to get table editor as `%s` is missing", foreignKey.ReferencedTableName)
		}
		exists, err := ContainsIndexedKey(ctx, referencingSte.tableEditor, indexKey, foreignKey.ReferencedTableIndex)
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
