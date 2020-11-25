// Copyright 2019 Dolthub, Inc.
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
	"errors"
	"fmt"
	"regexp"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	tableStructName = "table"

	schemaRefKey       = "schema_ref"
	tableRowsKey       = "rows"
	conflictsKey       = "conflicts"
	conflictSchemasKey = "conflict_schemas"
	indexesKey         = "indexes"
	autoIncrementKey   = "auto_increment"

	// TableNameRegexStr is the regular expression that valid tables must match.
	TableNameRegexStr = `^[a-zA-Z]{1}$|^[a-zA-Z]+[-_0-9a-zA-Z]*[0-9a-zA-Z]+$`
)

var tableNameRegex, _ = regexp.Compile(TableNameRegexStr)

// IsValidTableName returns true if the name matches the regular expression TableNameRegexStr.
// Table names must be composed of 1 or more letters and non-initial numerals, as well as the characters _ and -
func IsValidTableName(name string) bool {
	return tableNameRegex.MatchString(name)
}

// Table is a struct which holds row data, as well as a reference to it's schema.
type Table struct {
	vrw         types.ValueReadWriter
	tableStruct types.Struct
}

// NewTable creates a noms Struct which stores the schema and the row data. If indexData is nil, then it is rebuilt.
func NewTable(ctx context.Context, vrw types.ValueReadWriter, schemaVal types.Value, rowData types.Map, indexData *types.Map) (*Table, error) {
	if indexData == nil {
		sch, err := encoding.UnmarshalSchemaNomsValue(ctx, rowData.Format(), schemaVal)
		if err != nil {
			return nil, err
		}
		indexesMap, err := types.NewMap(ctx, vrw)
		if err != nil {
			return nil, err
		}

		for _, index := range sch.Indexes().AllIndexes() {
			rebuiltIndexRowData, err := rebuildIndexRowData(ctx, vrw, sch, rowData, index)
			if err != nil {
				return nil, err
			}
			rebuiltIndexRowDataRef, err := writeValAndGetRef(ctx, vrw, rebuiltIndexRowData)
			if err != nil {
				return nil, err
			}
			indexesMap, err = indexesMap.Edit().Set(types.String(index.Name()), rebuiltIndexRowDataRef).Map(ctx)
			if err != nil {
				return nil, err
			}
		}

		indexData = &indexesMap
	}

	schemaRef, err := writeValAndGetRef(ctx, vrw, schemaVal)
	if err != nil {
		return nil, err
	}

	rowDataRef, err := writeValAndGetRef(ctx, vrw, rowData)
	if err != nil {
		return nil, err
	}

	indexesRef, err := writeValAndGetRef(ctx, vrw, indexData)
	if err != nil {
		return nil, err
	}

	sd := types.StructData{
		schemaRefKey: schemaRef,
		tableRowsKey: rowDataRef,
		indexesKey:   indexesRef,
	}

	tableStruct, err := types.NewStruct(vrw.Format(), tableStructName, sd)
	if err != nil {
		return nil, err
	}

	return &Table{vrw, tableStruct}, nil
}

func (t *Table) Format() *types.NomsBinFormat {
	return t.vrw.Format()
}

// ValueReadWriter returns the ValueReadWriter for this table.
func (t *Table) ValueReadWriter() types.ValueReadWriter {
	return t.vrw
}

func (t *Table) SetConflicts(ctx context.Context, schemas Conflict, conflictData types.Map) (*Table, error) {
	conflictsRef, err := writeValAndGetRef(ctx, t.vrw, conflictData)

	if err != nil {
		return nil, err
	}

	tpl, err := schemas.ToNomsList(t.vrw)

	if err != nil {
		return nil, err
	}

	updatedSt, err := t.tableStruct.Set(conflictSchemasKey, tpl)

	if err != nil {
		return nil, err
	}

	updatedSt, err = updatedSt.Set(conflictsKey, conflictsRef)

	if err != nil {
		return nil, err
	}

	return &Table{t.vrw, updatedSt}, nil
}

func (t *Table) GetConflicts(ctx context.Context) (Conflict, types.Map, error) {
	schemasVal, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)

	if err != nil {
		return Conflict{}, types.EmptyMap, err
	}

	if !ok {
		return Conflict{}, types.EmptyMap, ErrNoConflicts
	}

	schemas, err := ConflictFromTuple(schemasVal.(types.Tuple))

	if err != nil {
		return Conflict{}, types.EmptyMap, err
	}

	conflictsVal, _, err := t.tableStruct.MaybeGet(conflictsKey)

	if err != nil {
		return Conflict{}, types.EmptyMap, err
	}

	confMap := types.EmptyMap
	if conflictsVal != nil {
		confMapRef := conflictsVal.(types.Ref)
		v, err := confMapRef.TargetValue(ctx, t.vrw)

		if err != nil {
			return Conflict{}, types.EmptyMap, err
		}

		confMap = v.(types.Map)
	}

	return schemas, confMap, nil
}

func (t *Table) HasConflicts() (bool, error) {
	if t == nil {
		return false, nil
	}

	_, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)

	return ok, err
}

func (t *Table) NumRowsInConflict(ctx context.Context) (uint64, error) {
	if t == nil {
		return 0, nil
	}

	conflictsVal, ok, err := t.tableStruct.MaybeGet(conflictsKey)

	if err != nil {
		return 0, err
	}

	if !ok {
		return 0, nil
	}

	confMap := types.EmptyMap
	if conflictsVal != nil {
		confMapRef := conflictsVal.(types.Ref)
		v, err := confMapRef.TargetValue(ctx, t.vrw)

		if err != nil {
			return 0, err
		}
		confMap = v.(types.Map)
	}

	return confMap.Len(), nil
}

func (t *Table) ClearConflicts() (*Table, error) {
	tSt, err := t.tableStruct.Delete(conflictSchemasKey)

	if err != nil {
		return nil, err
	}

	tSt, err = tSt.Delete(conflictsKey)

	if err != nil {
		return nil, err
	}

	return &Table{t.vrw, tSt}, nil
}

func (t *Table) GetConflictSchemas(ctx context.Context) (base, sch, mergeSch schema.Schema, err error) {
	schemasVal, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)

	if err != nil {
		return nil, nil, nil, err
	}

	if ok {
		schemas, err := ConflictFromTuple(schemasVal.(types.Tuple))

		if err != nil {
			return nil, nil, nil, err
		}

		baseRef := schemas.Base.(types.Ref)
		valRef := schemas.Value.(types.Ref)
		mergeRef := schemas.MergeValue.(types.Ref)

		var baseSch, sch, mergeSch schema.Schema
		if baseSch, err = RefToSchema(ctx, t.vrw, baseRef); err == nil {
			if sch, err = RefToSchema(ctx, t.vrw, valRef); err == nil {
				mergeSch, err = RefToSchema(ctx, t.vrw, mergeRef)
			}
		}

		return baseSch, sch, mergeSch, err
	}
	return nil, nil, nil, ErrNoConflicts
}

func RefToSchema(ctx context.Context, vrw types.ValueReadWriter, ref types.Ref) (schema.Schema, error) {
	schemaVal, err := ref.TargetValue(ctx, vrw)

	if err != nil {
		return nil, err
	}

	schema, err := encoding.UnmarshalSchemaNomsValue(ctx, vrw.Format(), schemaVal)

	if err != nil {
		return nil, err
	}

	return schema, nil
}

// GetSchema will retrieve the schema being referenced from the table in noms and unmarshal it.
func (t *Table) GetSchema(ctx context.Context) (schema.Schema, error) {
	schemaRefVal, _, err := t.tableStruct.MaybeGet(schemaRefKey)

	if err != nil {
		return nil, err
	}

	schemaRef := schemaRefVal.(types.Ref)
	return RefToSchema(ctx, t.vrw, schemaRef)
}

func (t *Table) GetSchemaRef() (types.Ref, error) {
	v, _, err := t.tableStruct.MaybeGet(schemaRefKey)

	if err != nil {
		return types.Ref{}, err
	}

	if v == nil {
		return types.Ref{}, errors.New("missing schema")
	}

	return v.(types.Ref), nil
}

// UpdateSchema updates the table with the schema given and returns the updated table. The original table is unchanged.
func (t *Table) UpdateSchema(ctx context.Context, sch schema.Schema) (*Table, error) {
	newSchemaVal, err := encoding.MarshalSchemaAsNomsValue(ctx, t.vrw, sch)
	if err != nil {
		return nil, err
	}
	rowData, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	indexData, err := t.GetIndexData(ctx)
	if err != nil {
		return nil, err
	}
	newTable, err := NewTable(ctx, t.vrw, newSchemaVal, rowData, &indexData)
	if err != nil {
		return nil, err
	}
	return newTable, nil
}

// HasTheSameSchema tests the schema within 2 tables for equality
func (t *Table) HasTheSameSchema(t2 *Table) (bool, error) {
	schemaVal, _, err := t.tableStruct.MaybeGet(schemaRefKey)

	if err != nil {
		return false, err
	}

	schemaRef := schemaVal.(types.Ref)

	schema2Val, _, err := t2.tableStruct.MaybeGet(schemaRefKey)

	if err != nil {
		return false, err
	}

	schema2Ref := schema2Val.(types.Ref)

	return schemaRef.TargetHash() == schema2Ref.TargetHash(), nil
}

// HashOf returns the hash of the underlying table struct
func (t *Table) HashOf() (hash.Hash, error) {
	return t.tableStruct.Hash(t.vrw.Format())
}

func (t *Table) GetRowByPKVals(ctx context.Context, pkVals row.TaggedValues, sch schema.Schema) (row.Row, bool, error) {
	pkTuple := pkVals.NomsTupleForPKCols(t.vrw.Format(), sch.GetPKCols())
	pkTupleVal, err := pkTuple.Value(ctx)

	if err != nil {
		return nil, false, err
	}

	return t.GetRow(ctx, pkTupleVal.(types.Tuple), sch)
}

// GetRow uses the noms DestRef containing the row data to lookup a row by primary key.  If a valid row exists with this pk
// then the supplied TableRowFactory will be used to create a TableRow using the row data.
func (t *Table) GetRow(ctx context.Context, pk types.Tuple, sch schema.Schema) (row.Row, bool, error) {
	rowMap, err := t.GetRowData(ctx)

	if err != nil {
		return nil, false, err
	}

	fieldsVal, _, err := rowMap.MaybeGet(ctx, pk)

	if err != nil {
		return nil, false, err
	}

	if fieldsVal == nil {
		return nil, false, nil
	}

	r, err := row.FromNoms(sch, pk, fieldsVal.(types.Tuple))

	if err != nil {
		return nil, false, err
	}

	return r, true, nil
}

// GetRows takes in a PKItr which will supply a stream of primary keys to be pulled from the table.  Each key is
// looked up sequentially.  If row data exists for a given pk it is converted to a TableRow, and added to the rows
// slice. If row data does not exist for a given pk it will be added to the missing slice.  The numPKs argument, if
// known helps allocate the right amount of memory for the results, but if the number of pks being requested isn't
// known then 0 can be used.
func (t *Table) GetRows(ctx context.Context, pkItr PKItr, numPKs int, sch schema.Schema) (rows []row.Row, missing []types.Value, err error) {
	if numPKs < 0 {
		numPKs = 0
	}

	rows = make([]row.Row, 0, numPKs)
	missing = make([]types.Value, 0, numPKs)

	rowMap, err := t.GetRowData(ctx)

	if err != nil {
		return nil, nil, err
	}

	for pk, ok, err := pkItr(); ok; pk, ok, err = pkItr() {
		if err != nil {
			return nil, nil, err
		}

		fieldsVal, _, err := rowMap.MaybeGet(ctx, pk)

		if err != nil {
			return nil, nil, err
		}

		if fieldsVal == nil {
			missing = append(missing, pk)
		} else {
			r, err := row.FromNoms(sch, pk, fieldsVal.(types.Tuple))

			if err != nil {
				return nil, nil, err
			}

			rows = append(rows, r)
		}
	}

	return rows, missing, nil
}

// UpdateRows replaces the current row data and returns and updated Table.  Calls to UpdateRows will not be written to the
// database.  The root must be updated with the updated table, and the root must be committed or written.
func (t *Table) UpdateRows(ctx context.Context, updatedRows types.Map) (*Table, error) {
	rowDataRef, err := writeValAndGetRef(ctx, t.vrw, updatedRows)

	if err != nil {
		return nil, err
	}

	updatedSt, err := t.tableStruct.Set(tableRowsKey, rowDataRef)

	if err != nil {
		return nil, err
	}

	return &Table{t.vrw, updatedSt}, nil
}

// GetRowData retrieves the underlying map which is a map from a primary key to a list of field values.
func (t *Table) GetRowData(ctx context.Context) (types.Map, error) {
	val, _, err := t.tableStruct.MaybeGet(tableRowsKey)

	if err != nil {
		return types.EmptyMap, err
	}

	rowMapRef := val.(types.Ref)

	val, err = rowMapRef.TargetValue(ctx, t.vrw)

	if err != nil {
		return types.EmptyMap, err
	}

	rowMap := val.(types.Map)
	return rowMap, nil
}

func (t *Table) ResolveConflicts(ctx context.Context, pkTuples []types.Value) (invalid, notFound []types.Value, tbl *Table, err error) {
	removed := 0
	_, confData, err := t.GetConflicts(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	confEdit := confData.Edit()
	for _, pkTupleVal := range pkTuples {
		if has, err := confData.Has(ctx, pkTupleVal); err != nil {
			return nil, nil, nil, err
		} else if has {
			removed++
			confEdit.Remove(pkTupleVal)
		} else {
			notFound = append(notFound, pkTupleVal)
		}
	}

	if removed == 0 {
		return invalid, notFound, tbl, nil
	}

	conflicts, err := confEdit.Map(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	conflictsRef, err := writeValAndGetRef(ctx, t.vrw, conflicts)

	if err != nil {
		return nil, nil, nil, err
	}

	updatedSt, err := t.tableStruct.Set(conflictsKey, conflictsRef)

	if err != nil {
		return nil, nil, nil, err
	}

	return invalid, notFound, &Table{t.vrw, updatedSt}, nil
}

// GetIndexData returns the internal index map which goes from index name to a ref of the row data map.
func (t *Table) GetIndexData(ctx context.Context) (types.Map, error) {
	indexesVal, ok, err := t.tableStruct.MaybeGet(indexesKey)
	if err != nil {
		return types.EmptyMap, err
	}
	if !ok {
		newEmptyMap, err := types.NewMap(ctx, t.vrw)
		if err != nil {
			return types.EmptyMap, err
		}
		return newEmptyMap, nil
	}

	indexesMap, err := indexesVal.(types.Ref).TargetValue(ctx, t.vrw)
	if err != nil {
		return types.EmptyMap, err
	}

	return indexesMap.(types.Map), nil
}

// RebuildIndexData rebuilds all of the data for each index, and returns an updated Table.
func (t *Table) RebuildIndexData(ctx context.Context) (*Table, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if sch.Indexes().Count() == 0 {
		return t, nil
	}

	tableRowData, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	indexesMap, err := t.GetIndexData(ctx)
	if err != nil {
		return nil, err
	}

	for _, index := range sch.Indexes().AllIndexes() {
		rebuiltIndexRowData, err := rebuildIndexRowData(ctx, t.vrw, sch, tableRowData, index)
		if err != nil {
			return nil, err
		}
		rebuiltIndexRowDataRef, err := writeValAndGetRef(ctx, t.vrw, rebuiltIndexRowData)
		if err != nil {
			return nil, err
		}
		indexesMap, err = indexesMap.Edit().Set(types.String(index.Name()), rebuiltIndexRowDataRef).Map(ctx)
		if err != nil {
			return nil, err
		}
	}

	return t.SetIndexData(ctx, indexesMap)
}

// SetIndexData replaces the current internal index map, and returns an updated Table.
func (t *Table) SetIndexData(ctx context.Context, indexesMap types.Map) (*Table, error) {
	indexesRef, err := writeValAndGetRef(ctx, t.vrw, indexesMap)
	if err != nil {
		return nil, err
	}

	newTableStruct, err := t.tableStruct.Set(indexesKey, indexesRef)
	if err != nil {
		return nil, err
	}

	return &Table{t.vrw, newTableStruct}, nil
}

// GetIndexRowData retrieves the underlying map of an index, in which the primary key consists of all indexed columns.
func (t *Table) GetIndexRowData(ctx context.Context, indexName string) (types.Map, error) {
	indexesMap, err := t.GetIndexData(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	indexMapRef, ok, err := indexesMap.MaybeGet(ctx, types.String(indexName))
	if err != nil {
		return types.EmptyMap, err
	}
	if !ok {
		return types.EmptyMap, fmt.Errorf("index `%s` is missing its data", indexName)
	}

	indexMap, err := indexMapRef.(types.Ref).TargetValue(ctx, t.vrw)
	if err != nil {
		return types.EmptyMap, err
	}

	return indexMap.(types.Map), nil
}

// RebuildIndexRowData rebuilds the data for the given index, and returns the updated Map.
func (t *Table) RebuildIndexRowData(ctx context.Context, indexName string) (types.Map, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	tableRowData, err := t.GetRowData(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	index := sch.Indexes().GetByName(indexName)
	if index == nil {
		return types.EmptyMap, fmt.Errorf("index `%s` does not exist", indexName)
	}

	rebuiltIndexData, err := rebuildIndexRowData(ctx, t.vrw, sch, tableRowData, index)
	if err != nil {
		return types.EmptyMap, err
	}
	return rebuiltIndexData, nil
}

// SetIndexRowData replaces the current row data for the given index and returns an updated Table.
func (t *Table) SetIndexRowData(ctx context.Context, indexName string, indexRowData types.Map) (*Table, error) {
	indexesMap, err := t.GetIndexData(ctx)
	if err != nil {
		return nil, err
	}

	indexRowDataRef, err := writeValAndGetRef(ctx, t.vrw, indexRowData)
	if err != nil {
		return nil, err
	}
	indexesMap, err = indexesMap.Edit().Set(types.String(indexName), indexRowDataRef).Map(ctx)
	if err != nil {
		return nil, err
	}

	return t.SetIndexData(ctx, indexesMap)
}

// DeleteIndexRowData removes the underlying map of an index, along with its key entry. This should only be used
// when removing an index altogether. If the intent is to clear an index's data, then use SetIndexRowData with
// an empty map.
func (t *Table) DeleteIndexRowData(ctx context.Context, indexName string) (*Table, error) {
	indexesMap, err := t.GetIndexData(ctx)
	if err != nil {
		return nil, err
	}

	key := types.String(indexName)
	if has, err := indexesMap.Has(ctx, key); err != nil {
		return nil, err
	} else if has {
		indexesMap, err = indexesMap.Edit().Remove(key).Map(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		return t, nil
	}

	return t.SetIndexData(ctx, indexesMap)
}

// RenameIndexRowData changes the name for the index data. Does not verify that the new name is unoccupied. If the old
// name does not exist, then this returns the called table without error.
func (t *Table) RenameIndexRowData(ctx context.Context, oldIndexName, newIndexName string) (*Table, error) {
	indexesMap, err := t.GetIndexData(ctx)
	if err != nil {
		return nil, err
	}

	oldKey := types.String(oldIndexName)
	newKey := types.String(newIndexName)
	if indexRowData, ok, err := indexesMap.MaybeGet(ctx, oldKey); err != nil {
		return nil, err
	} else if ok {
		indexesMap, err = indexesMap.Edit().Set(newKey, indexRowData).Remove(oldKey).Map(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		return t, nil
	}

	return t.SetIndexData(ctx, indexesMap)
}

// VerifyIndexRowData verifies that the index with the given name's data matches what the index expects.
func (t *Table) VerifyIndexRowData(ctx context.Context, indexName string) error {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return err
	}

	index := sch.Indexes().GetByName(indexName)
	if index == nil {
		return fmt.Errorf("index `%s` does not exist", indexName)
	}

	indexesMap, err := t.GetIndexData(ctx)
	if err != nil {
		return err
	}

	indexMapRef, ok, err := indexesMap.MaybeGet(ctx, types.String(indexName))
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("index `%s` is missing its data", indexName)
	}

	indexMapValue, err := indexMapRef.(types.Ref).TargetValue(ctx, t.vrw)
	if err != nil {
		return err
	}

	iter, err := indexMapValue.(types.Map).Iterator(ctx)
	if err != nil {
		return err
	}

	return index.VerifyMap(ctx, iter, indexMapValue.(types.Map).Format())
}

func rebuildIndexRowData(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, tblRowData types.Map, index schema.Index) (types.Map, error) {
	emptyIndexMap, err := types.NewMap(ctx, vrw)
	if err != nil {
		return types.EmptyMap, err
	}
	indexEditor := NewIndexEditor(index, emptyIndexMap)

	err = tblRowData.IterAll(ctx, func(key, value types.Value) error {
		dRow, err := row.FromNoms(sch, key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return err
		}
		indexRow, err := dRow.ReduceToIndex(index)
		if err != nil {
			return err
		}
		err = indexEditor.UpdateIndex(ctx, nil, indexRow)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return types.EmptyMap, err
	}

	rebuiltIndexMap, err := indexEditor.Map(ctx)
	if err != nil {
		return types.EmptyMap, err
	}
	return rebuiltIndexMap, nil
}

func (t *Table) GetAutoIncrementValue(ctx context.Context) (types.Value, error) {
	val, ok, err := t.tableStruct.MaybeGet(autoIncrementKey)
	if err != nil {
		return nil, err
	}
	if ok {
		return val, nil
	}

	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	kind := types.UnknownKind
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.AutoIncrement {
			kind = col.Kind
			stop = true
		}
		return
	})
	switch kind {
	case types.IntKind:
		return types.Int(1), nil
	case types.UintKind:
		return types.Uint(1), nil
	case types.FloatKind:
		return types.Float(1), nil
	default:
		return nil, fmt.Errorf("auto increment set for non-numeric column type")
	}
}

func (t *Table) SetAutoIncrementValue(val types.Value) (*Table, error) {
	switch val.(type) {
	case types.Int, types.Uint, types.Float:
		st, err := t.tableStruct.Set(autoIncrementKey, val)
		if err != nil {
			return nil, err
		}
		return &Table{t.vrw, st}, nil

	default:
		return nil, fmt.Errorf("cannot set auto increment to non-numeric value")
	}
}
