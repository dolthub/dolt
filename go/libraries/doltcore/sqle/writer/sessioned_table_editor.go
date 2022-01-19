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

package writer

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// sessionedTableEditor represents a table editor obtained from a nomsWriteSession. This table editor may be shared
// by multiple callers. It is thread safe.
type sessionedTableEditor struct {
	tableEditSession  *nomsWriteSession
	tableEditor       editor.TableEditor
	referencedTables  []doltdb.ForeignKey // The tables that we reference to ensure an insert or update is valid
	referencingTables []doltdb.ForeignKey // The tables that reference us to ensure their inserts and updates are valid
	indexSchemaCache  map[string]schema.Schema
	dirty             bool
}

var _ editor.TableEditor = &sessionedTableEditor{}

func (ste *sessionedTableEditor) InsertKeyVal(ctx context.Context, key, val types.Tuple, tagToVal map[uint64]types.Value, errFunc editor.PKDuplicateErrFunc) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	err := ste.validateForInsert(ctx, tagToVal)
	if err != nil {
		return err
	}

	ste.dirty = true
	return ste.tableEditor.InsertKeyVal(ctx, key, val, tagToVal, errFunc)
}

func (ste *sessionedTableEditor) DeleteByKey(ctx context.Context, key types.Tuple, tagToVal map[uint64]types.Value) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	if !ste.tableEditSession.opts.ForeignKeyChecksDisabled && len(ste.referencingTables) > 0 {
		err := ste.onDeleteHandleRowsReferencingValues(ctx, key, tagToVal)
		if err != nil {
			return err
		}
	}

	ste.dirty = true
	return ste.tableEditor.DeleteByKey(ctx, key, tagToVal)
}

// InsertRow adds the given row to the table. If the row already exists, use UpdateRow.
func (ste *sessionedTableEditor) InsertRow(ctx context.Context, dRow row.Row, errFunc editor.PKDuplicateErrFunc) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	dRowTaggedVals, err := dRow.TaggedValues()
	if err != nil {
		return err
	}
	err = ste.validateForInsert(ctx, dRowTaggedVals)
	if err != nil {
		return err
	}

	ste.dirty = true
	return ste.tableEditor.InsertRow(ctx, dRow, errFunc)
}

// DeleteRow removes the given key from the table.
func (ste *sessionedTableEditor) DeleteRow(ctx context.Context, r row.Row) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	if !ste.tableEditSession.opts.ForeignKeyChecksDisabled && len(ste.referencingTables) > 0 {
		err := ste.handleReferencingRowsOnDelete(ctx, r)
		if err != nil {
			return err
		}
	}

	ste.dirty = true
	return ste.tableEditor.DeleteRow(ctx, r)
}

// UpdateRow takes the current row and new row, and updates it accordingly. Any applicable rows from tables that have a
// foreign key referencing this table will also be updated.
func (ste *sessionedTableEditor) UpdateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row, errFunc editor.PKDuplicateErrFunc) error {
	ste.tableEditSession.writeMutex.RLock()
	defer ste.tableEditSession.writeMutex.RUnlock()

	return ste.updateRow(ctx, dOldRow, dNewRow, true, errFunc)
}

// hasEdits returns whether the table editor has had any write operations, whether they were successful or unsuccessful
// (on the underlying table editor). This makes it possible for this to return true when the table editor does not
// actually contain any new edits, which is preferable to potentially returning false when there are edits.
func (ste *sessionedTableEditor) HasEdits() bool {
	if ste.dirty {
		return true
	}
	return ste.tableEditor.HasEdits()
}

// GetAutoIncrementValue implements TableEditor.
func (ste *sessionedTableEditor) GetAutoIncrementValue() types.Value {
	return ste.tableEditor.GetAutoIncrementValue()
}

// SetAutoIncrementValue implements TableEditor.
func (ste *sessionedTableEditor) SetAutoIncrementValue(v types.Value) error {
	ste.dirty = true
	return ste.tableEditor.SetAutoIncrementValue(v)
}

// Table implements TableEditor.
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

// Schema implements TableEditor.
func (ste *sessionedTableEditor) Schema() schema.Schema {
	return ste.tableEditor.Schema()
}

// Name implements TableEditor.
func (ste *sessionedTableEditor) Name() string {
	return ste.tableEditor.Name()
}

// Format implements TableEditor.
func (ste *sessionedTableEditor) Format() *types.NomsBinFormat {
	return ste.tableEditor.Format()
}

// ValueReadWriter implements TableEditor.
func (ste *sessionedTableEditor) ValueReadWriter() types.ValueReadWriter {
	return ste.tableEditor.ValueReadWriter()
}

// StatementStarted implements TableEditor.
func (ste *sessionedTableEditor) StatementStarted(ctx context.Context) {
	ste.tableEditor.StatementStarted(ctx)
}

// StatementFinished implements TableEditor.
func (ste *sessionedTableEditor) StatementFinished(ctx context.Context, errored bool) error {
	return ste.tableEditor.StatementFinished(ctx, errored)
}

// SetConstraintViolation implements TableEditor.
func (ste *sessionedTableEditor) SetConstraintViolation(ctx context.Context, k types.LesserValuable, v types.Valuable) error {
	ste.dirty = true
	return ste.tableEditor.SetConstraintViolation(ctx, k, v)
}

// Close implements TableEditor.
func (ste *sessionedTableEditor) Close(ctx context.Context) error {
	return ste.tableEditor.Close(ctx)
}

// handleReferencingRowsOnDelete handles updating referencing foreign keys on delete operations
func (ste *sessionedTableEditor) handleReferencingRowsOnDelete(ctx context.Context, dRow row.Row) error {
	dRowTaggedVals, err := dRow.TaggedValues()
	if err != nil {
		return err
	}

	key, err := dRow.NomsMapKey(ste.tableEditor.Schema()).Value(ctx)
	if err != nil {
		return err
	}

	return ste.onDeleteHandleRowsReferencingValues(ctx, key.(types.Tuple), dRowTaggedVals)
}

func (ste *sessionedTableEditor) onDeleteHandleRowsReferencingValues(ctx context.Context, key types.Tuple, dRowTaggedVals row.TaggedValues) error {
	//TODO: all self referential logic assumes non-composite keys
	if ste.tableEditSession.opts.ForeignKeyChecksDisabled {
		return nil
	}

	if ste.indexSchemaCache == nil {
		ste.indexSchemaCache = make(map[string]schema.Schema)
	}

	nbf := ste.Format()
	for _, foreignKey := range ste.referencingTables {
		if !foreignKey.IsResolved() {
			return sql.ErrForeignKeyNotResolved.New(
				ste.Name(),
				foreignKey.Name,
				strings.Join(foreignKey.UnresolvedFKDetails.TableColumns, "`, `"),
				foreignKey.ReferencedTableName,
				strings.Join(foreignKey.UnresolvedFKDetails.ReferencedTableColumns, "`, `"))
		}

		referencingSte, ok := ste.tableEditSession.tables[foreignKey.TableName]
		if !ok {
			return fmt.Errorf("unable to get table editor as `%s` is missing", foreignKey.TableName)
		}
		indexKey, hasNulls, err := ste.reduceRowAndConvert(nbf, foreignKey.ReferencedTableColumns, foreignKey.TableColumns, dRowTaggedVals)
		if err != nil {
			return err
		}
		if hasNulls {
			continue
		}

		tblName := referencingSte.tableEditor.Name()
		cacheKey := tblName + "->" + foreignKey.TableIndex
		idxSch, ok := ste.indexSchemaCache[cacheKey]
		if !ok {
			idxSch = referencingSte.tableEditor.Schema().Indexes().GetByName(foreignKey.TableIndex).Schema()
			ste.indexSchemaCache[cacheKey] = idxSch
		}

		referencingRowKVPs, err := editor.GetIndexedRowKVPs(ctx, referencingSte.tableEditor, indexKey, foreignKey.TableIndex, idxSch)
		if err != nil {
			return err
		}
		if len(referencingRowKVPs) == 0 {
			continue
		}

		var shouldSkip bool
		switch foreignKey.OnDelete {
		case doltdb.ForeignKeyReferenceOption_Cascade:
			for _, kvpToDelete := range referencingRowKVPs {
				ctx, shouldSkip, err = ste.shouldSkipDeleteCascade(ctx, foreignKey, key, kvpToDelete[0])
				if err != nil {
					return err
				}
				if shouldSkip {
					continue
				}
				taggedVals, err := row.TaggedValuesFromTupleKeyAndValue(kvpToDelete[0], kvpToDelete[1])
				if err != nil {
					return err
				}

				err = referencingSte.DeleteByKey(ctx, kvpToDelete[0], taggedVals)
				if err != nil {
					return err
				}
			}
		case doltdb.ForeignKeyReferenceOption_SetNull:
			for _, unalteredNewKVP := range referencingRowKVPs {
				taggedVals, err := row.TaggedValuesFromTupleKeyAndValue(unalteredNewKVP[0], unalteredNewKVP[1])
				if err != nil {
					return err
				}

				sch := referencingSte.tableEditor.Schema()
				oldRow, err := row.FromNoms(sch, unalteredNewKVP[0], unalteredNewKVP[1])
				if err != nil {
					return err
				}

				if foreignKey.IsSelfReferential() && row.TaggedValsEqualForSch(taggedVals, dRowTaggedVals, sch) {
					continue
				}

				for _, colTag := range foreignKey.TableColumns {
					taggedVals[colTag] = types.NullValue
				}

				newRow, err := taggedVals.ToRow(ctx, nbf, sch)
				if err != nil {
					return err
				}

				err = referencingSte.updateRow(ctx, oldRow, newRow, false, nil)
				if err != nil {
					return err
				}
			}
		case doltdb.ForeignKeyReferenceOption_DefaultAction, doltdb.ForeignKeyReferenceOption_NoAction, doltdb.ForeignKeyReferenceOption_Restrict:
			indexKeyStr, _ := formatKey(ctx, indexKey)
			return sql.ErrForeignKeyChildViolation.New(foreignKey.Name, foreignKey.TableName, foreignKey.ReferencedTableName, indexKeyStr)
		default:
			return fmt.Errorf("unknown ON DELETE reference option on `%s`: `%s`", foreignKey.Name, foreignKey.OnDelete.String())
		}
	}

	return nil
}

func (ste *sessionedTableEditor) handleReferencingRowsOnUpdate(ctx context.Context, dOldRow row.Row, dNewRow row.Row) error {
	//TODO: all self referential logic assumes non-composite keys
	if ste.tableEditSession.opts.ForeignKeyChecksDisabled {
		return nil
	}
	dOldRowTaggedVals, err := dOldRow.TaggedValues()
	if err != nil {
		return err
	}
	if ste.indexSchemaCache == nil {
		ste.indexSchemaCache = make(map[string]schema.Schema)
	}

	nbf := ste.Format()
	for _, foreignKey := range ste.referencingTables {
		if !foreignKey.IsResolved() {
			return sql.ErrForeignKeyNotResolved.New(
				ste.Name(),
				foreignKey.Name,
				strings.Join(foreignKey.UnresolvedFKDetails.TableColumns, "`, `"),
				foreignKey.ReferencedTableName,
				strings.Join(foreignKey.UnresolvedFKDetails.ReferencedTableColumns, "`, `"))
		}

		referencingSte, ok := ste.tableEditSession.tables[foreignKey.TableName]
		if !ok {
			return fmt.Errorf("unable to get table editor as `%s` is missing", foreignKey.TableName)
		}
		indexKey, hasNulls, err := ste.reduceRowAndConvert(nbf, foreignKey.ReferencedTableColumns, foreignKey.TableColumns, dOldRowTaggedVals)
		if err != nil {
			return err
		}
		if hasNulls {
			continue
		}
		tblName := referencingSte.tableEditor.Name()
		cacheKey := tblName + "->" + foreignKey.TableIndex
		idxSch, ok := ste.indexSchemaCache[cacheKey]
		if !ok {
			idxSch = referencingSte.tableEditor.Schema().Indexes().GetByName(foreignKey.TableIndex).Schema()
			ste.indexSchemaCache[cacheKey] = idxSch
		}
		referencingRows, err := editor.GetIndexedRows(ctx, referencingSte.tableEditor, indexKey, foreignKey.TableIndex, idxSch)
		if err != nil {
			return err
		}
		if len(referencingRows) == 0 {
			continue
		}

		valueChanged := false
		for _, colTag := range foreignKey.ReferencedTableColumns {
			oldVal, oldOk := dOldRowTaggedVals[colTag]
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
			if foreignKey.IsSelfReferential() {
				return fmt.Errorf("foreign key constraint violation on `%s`.`%s`: cascading updates always violate self referential foreign keys",
					foreignKey.TableName, foreignKey.Name)
			}
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
				err = referencingSte.updateRow(ctx, rowToUpdate, newRow, false, nil)
				if err != nil {
					return err
				}
			}
		case doltdb.ForeignKeyReferenceOption_SetNull:
			if foreignKey.IsSelfReferential() {
				return fmt.Errorf("foreign key constraint violation on `%s`.`%s`: SET NULL updates always violate self referential foreign keys",
					foreignKey.TableName, foreignKey.Name)
			}
			for _, oldRow := range referencingRows {
				newRow := oldRow
				for _, colTag := range foreignKey.TableColumns {
					newRow, err = newRow.SetColVal(colTag, types.NullValue, referencingSte.tableEditor.Schema())
					if err != nil {
						return err
					}
				}
				err = referencingSte.updateRow(ctx, oldRow, newRow, false, nil)
				if err != nil {
					return err
				}
			}
		case doltdb.ForeignKeyReferenceOption_DefaultAction, doltdb.ForeignKeyReferenceOption_NoAction, doltdb.ForeignKeyReferenceOption_Restrict:
			indexKeyStr, _ := formatKey(ctx, indexKey)
			return sql.ErrForeignKeyParentViolation.New(foreignKey.Name, foreignKey.TableName, foreignKey.ReferencedTableName, indexKeyStr)
		default:
			return fmt.Errorf("unknown ON UPDATE reference option on `%s`: `%s`", foreignKey.Name, foreignKey.OnUpdate.String())
		}
	}

	return nil
}

// shouldSkipDeleteCascade determines whether the next row should be deleted, based on if a loop has been detected in
// cascading deletes. Stores the previous delete hashes in the context, and returns a new context if the old one did not
// have any hashes. Only applies to self referential foreign keys.
func (ste *sessionedTableEditor) shouldSkipDeleteCascade(ctx context.Context, foreignKey doltdb.ForeignKey, oldKey, newKey types.Tuple) (context.Context, bool, error) {
	//TODO: all self referential logic assumes non-composite keys
	if !foreignKey.IsSelfReferential() {
		return ctx, false, nil
	}

	const contextValueName = "SELF_FOREIGN_KEY_DELETION"
	var deleteKeys map[hash.Hash]struct{}
	mapInterface := ctx.Value(contextValueName)
	if mapInterface != nil {
		deleteKeys = mapInterface.(map[hash.Hash]struct{})
	} else {
		deleteKeys = make(map[hash.Hash]struct{})
		ctx = context.WithValue(ctx, contextValueName, deleteKeys)
	}

	oldKeyHash, err := oldKey.Hash(ste.Format())
	if err != nil {
		return ctx, false, err
	}
	deleteKeys[oldKeyHash] = struct{}{}

	// We don't need to store the new key. If it also causes a cascade then it will become an old key as the logic
	// progresses. We're only interested in whether the new key is already present in the map.
	newKeyHash, err := newKey.Hash(ste.Format())
	if err != nil {
		return ctx, false, err
	}
	_, ok := deleteKeys[newKeyHash]
	return ctx, ok, nil
}

// reduceRowAndConvert takes in a row and returns a Tuple containing only the values from the tags given. The returned
// items have tags from newTags, while the tags from dRow are expected to match originalTags. Both parameter slices are
// assumed to have equivalent ordering and length. If the key contains any nulls, then we return true to indicate that
// we do not propagate an ON DELETE/UPDATE.
func (ste *sessionedTableEditor) reduceRowAndConvert(nbf *types.NomsBinFormat, originalTags []uint64, newTags []uint64, taggedVals row.TaggedValues) (types.Tuple, bool, error) {
	keyVals := make([]types.Value, len(originalTags)*2)
	for i, colTag := range originalTags {
		val, ok := taggedVals[colTag]
		if !ok || val == types.NullValue {
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

func (ste *sessionedTableEditor) updateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row, checkReferences bool, errFunc editor.PKDuplicateErrFunc) error {
	if checkReferences {
		dNewRowTaggedVals, err := dNewRow.TaggedValues()
		if err != nil {
			return err
		}
		err = ste.validateForInsert(ctx, dNewRowTaggedVals)
		if err != nil {
			return err
		}
	}

	err := ste.handleReferencingRowsOnUpdate(ctx, dOldRow, dNewRow)
	if err != nil {
		return err
	}

	ste.dirty = true
	return ste.tableEditor.UpdateRow(ctx, dOldRow, dNewRow, errFunc)
}

// validateForInsert returns whether the given row is able to be inserted into the target table.
func (ste *sessionedTableEditor) validateForInsert(ctx context.Context, taggedVals row.TaggedValues) error {
	if ste.tableEditSession.opts.ForeignKeyChecksDisabled {
		return nil
	}

	if ste.indexSchemaCache == nil {
		ste.indexSchemaCache = make(map[string]schema.Schema)
	}

	for _, foreignKey := range ste.referencedTables {
		if !foreignKey.IsResolved() {
			return sql.ErrForeignKeyNotResolved.New(
				ste.Name(),
				foreignKey.Name,
				strings.Join(foreignKey.UnresolvedFKDetails.TableColumns, "`, `"),
				foreignKey.ReferencedTableName,
				strings.Join(foreignKey.UnresolvedFKDetails.ReferencedTableColumns, "`, `"))
		}

		indexKey, hasNulls, err := ste.reduceRowAndConvert(ste.tableEditor.Format(), foreignKey.TableColumns, foreignKey.ReferencedTableColumns, taggedVals)
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

		tblName := referencingSte.tableEditor.Name()
		cacheKey := tblName + "->" + foreignKey.ReferencedTableIndex
		idxSch, ok := ste.indexSchemaCache[cacheKey]
		if !ok {
			idxSch = referencingSte.tableEditor.Schema().Indexes().GetByName(foreignKey.ReferencedTableIndex).Schema()
			ste.indexSchemaCache[cacheKey] = idxSch
		}

		exists, err := editor.ContainsIndexedKey(ctx, referencingSte.tableEditor, indexKey, foreignKey.ReferencedTableIndex, idxSch)
		if err != nil {
			return err
		}
		if !exists {
			if foreignKey.IsSelfReferential() {
				rowContainsValues := true
				for i := range foreignKey.TableColumns {
					val := taggedVals[foreignKey.TableColumns[i]] // Null & non-existent values are caught earlier
					refVal, ok := taggedVals[foreignKey.ReferencedTableColumns[i]]
					if !ok || !val.Equals(refVal) {
						rowContainsValues = false
						break
					}
				}
				if rowContainsValues {
					continue
				}
			}
			indexKeyStr, _ := formatKey(ctx, indexKey)
			return sql.ErrForeignKeyChildViolation.New(foreignKey.Name, foreignKey.TableName, foreignKey.ReferencedTableName, indexKeyStr)
		}
	}
	return nil
}

// formatKey returns a comma-separated string representation of the key given.
func formatKey(ctx context.Context, key types.Value) (string, error) {
	tuple, ok := key.(types.Tuple)
	if !ok {
		return "", fmt.Errorf("Expected types.Tuple but got %T", key)
	}

	var vals []string
	iter, err := tuple.Iterator()
	if err != nil {
		return "", err
	}

	for iter.HasMore() {
		i, val, err := iter.Next()
		if err != nil {
			return "", err
		}
		if i%2 == 1 {
			str, err := types.EncodedValue(ctx, val)
			if err != nil {
				return "", err
			}
			vals = append(vals, str)
		}
	}

	return fmt.Sprintf("[%s]", strings.Join(vals, ",")), nil
}
