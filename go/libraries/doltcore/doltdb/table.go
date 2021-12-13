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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	tableStructName = "table"

	schemaRefKey            = "schema_ref"
	tableRowsKey            = "rows"
	conflictsKey            = "conflicts"
	conflictSchemasKey      = "conflict_schemas"
	constraintViolationsKey = "constraint_violations"
	indexesKey              = "indexes"
	autoIncrementKey        = "auto_increment"

	// TableNameRegexStr is the regular expression that valid tables must match.
	TableNameRegexStr = `^[a-zA-Z]{1}$|^[a-zA-Z_]+[-_0-9a-zA-Z]*[0-9a-zA-Z]+$`
	// ForeignKeyNameRegexStr is the regular expression that valid foreign keys must match.
	// From the unquoted identifiers: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	// We also allow the '-' character from quoted identifiers.
	ForeignKeyNameRegexStr = `^[-$_0-9a-zA-Z]+$`
	// IndexNameRegexStr is the regular expression that valid indexes must match.
	// From the unquoted identifiers: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	// We also allow the '-' character from quoted identifiers.
	IndexNameRegexStr = `^[-$_0-9a-zA-Z]+$`
)

var (
	tableNameRegex      = regexp.MustCompile(TableNameRegexStr)
	foreignKeyNameRegex = regexp.MustCompile(ForeignKeyNameRegexStr)
	indexNameRegex      = regexp.MustCompile(IndexNameRegexStr)

	ErrNoConflictsResolved  = errors.New("no conflicts resolved")
	ErrNoAutoIncrementValue = fmt.Errorf("auto increment set for non-numeric column type")
)

// IsValidTableName returns true if the name matches the regular expression TableNameRegexStr.
// Table names must be composed of 1 or more letters and non-initial numerals, as well as the characters _ and -
func IsValidTableName(name string) bool {
	return tableNameRegex.MatchString(name)
}

// IsValidForeignKeyName returns true if the name matches the regular expression ForeignKeyNameRegexStr.
func IsValidForeignKeyName(name string) bool {
	return foreignKeyNameRegex.MatchString(name)
}

// IsValidIndexName returns true if the name matches the regular expression IndexNameRegexStr.
func IsValidIndexName(name string) bool {
	return indexNameRegex.MatchString(name)
}

// Table is a struct which holds row data, as well as a reference to it's schema.
type Table struct {
	vrw         types.ValueReadWriter
	tableStruct types.Struct
}

// NewTable creates a noms Struct which stores row data, index data, and schema.
func NewTable(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rowData types.Map, indexData types.Map, autoIncVal types.Value) (*Table, error) {
	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	if err != nil {
		return nil, err
	}

	schemaRef, err := WriteValAndGetRef(ctx, vrw, schVal)
	if err != nil {
		return nil, err
	}

	rowDataRef, err := WriteValAndGetRef(ctx, vrw, rowData)
	if err != nil {
		return nil, err
	}

	indexesRef, err := WriteValAndGetRef(ctx, vrw, indexData)
	if err != nil {
		return nil, err
	}

	sd := types.StructData{
		schemaRefKey: schemaRef,
		tableRowsKey: rowDataRef,
		indexesKey:   indexesRef,
	}

	if autoIncVal != nil {
		sd[autoIncrementKey] = autoIncVal
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

func (t *Table) SetConflicts(ctx context.Context, schemas ConflictSchema, conflictData types.Map) (*Table, error) {
	conflictsRef, err := WriteValAndGetRef(ctx, t.vrw, conflictData)
	if err != nil {
		return nil, err
	}

	tpl, err := ValueFromConflictSchema(ctx, t.vrw, schemas)
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

// GetConflicts returns a map built from ValueReadWriter when there are no conflicts in table
func (t *Table) GetConflicts(ctx context.Context) (ConflictSchema, types.Map, error) {
	schemasVal, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)
	if err != nil {
		return ConflictSchema{}, types.EmptyMap, err
	}
	if !ok {
		confMap, _ := types.NewMap(ctx, t.ValueReadWriter())
		return ConflictSchema{}, confMap, nil
	}

	schemas, err := ConflictSchemaFromValue(ctx, t.vrw, schemasVal)
	if err != nil {
		return ConflictSchema{}, types.EmptyMap, err
	}

	conflictsVal, _, err := t.tableStruct.MaybeGet(conflictsKey)
	if err != nil {
		return ConflictSchema{}, types.EmptyMap, err
	}

	confMap := types.EmptyMap
	if conflictsVal != nil {
		confMapRef := conflictsVal.(types.Ref)
		v, err := confMapRef.TargetValue(ctx, t.vrw)

		if err != nil {
			return ConflictSchema{}, types.EmptyMap, err
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
	return nil, nil, nil, nil
}

// GetConstraintViolationsSchema returns the schema for the dolt_constraint_violations system table belonging to this
// table.
func (t *Table) GetConstraintViolationsSchema(ctx context.Context) (schema.Schema, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	typeType, err := typeinfo.FromSqlType(
		sql.MustCreateEnumType([]string{"foreign key", "unique index", "check constraint"}, sql.Collation_Default))
	if err != nil {
		return nil, err
	}
	typeCol, err := schema.NewColumnWithTypeInfo("violation_type", schema.DoltConstraintViolationsTypeTag, typeType, true, "", false, "")
	if err != nil {
		return nil, err
	}
	infoCol, err := schema.NewColumnWithTypeInfo("violation_info", schema.DoltConstraintViolationsInfoTag, typeinfo.JSONType, false, "", false, "")
	if err != nil {
		return nil, err
	}

	colColl := schema.NewColCollection()
	colColl = colColl.Append(typeCol)
	colColl = colColl.Append(sch.GetAllCols().GetColumns()...)
	colColl = colColl.Append(infoCol)
	return schema.SchemaFromCols(colColl)
}

// GetConstraintViolations returns a map of all constraint violations for this table, along with a bool indicating
// whether the table has any violations.
func (t *Table) GetConstraintViolations(ctx context.Context) (types.Map, error) {
	constraintViolationsRefVal, ok, err := t.tableStruct.MaybeGet(constraintViolationsKey)
	if err != nil {
		return types.EmptyMap, err
	}
	if !ok {
		emptyMap, err := types.NewMap(ctx, t.vrw)
		return emptyMap, err
	}
	constraintViolationsVal, err := constraintViolationsRefVal.(types.Ref).TargetValue(ctx, t.vrw)
	if err != nil {
		return types.EmptyMap, err
	}
	return constraintViolationsVal.(types.Map), nil
}

// SetConstraintViolations sets this table's violations to the given map. If the map is empty, then the constraint
// violations entry on the embedded struct is removed.
func (t *Table) SetConstraintViolations(ctx context.Context, violationsMap types.Map) (*Table, error) {
	// We can't just call violationsMap.Empty() as we can't guarantee that the caller passed in an instantiated map
	if violationsMap == types.EmptyMap || violationsMap.Len() == 0 {
		updatedStruct, err := t.tableStruct.Delete(constraintViolationsKey)
		if err != nil {
			return nil, err
		}
		return &Table{t.vrw, updatedStruct}, nil
	}
	constraintViolationsRef, err := WriteValAndGetRef(ctx, t.vrw, violationsMap)
	if err != nil {
		return nil, err
	}
	updatedStruct, err := t.tableStruct.Set(constraintViolationsKey, constraintViolationsRef)
	if err != nil {
		return nil, err
	}
	return &Table{t.vrw, updatedStruct}, nil
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

func (t *Table) GetSchemaHash(ctx context.Context) (hash.Hash, error) {
	//v, _, err := t.tableStruct.MaybeGet(schemaRefKey)
	//
	//if err != nil {
	//	return types.Ref{}, err
	//}
	//
	//if v == nil {
	//	return types.Ref{}, errors.New("missing schema")
	//}
	//
	//return v.(types.Ref), nil
	return hash.Hash{}, nil
}

// UpdateSchema updates the table with the schema given and returns the updated table. The original table is unchanged.
func (t *Table) UpdateSchema(ctx context.Context, sch schema.Schema) (*Table, error) {
	newSchemaVal, err := encoding.MarshalSchemaAsNomsValue(ctx, t.vrw, sch)
	if err != nil {
		return nil, err
	}

	schRef, err := WriteValAndGetRef(ctx, t.vrw, newSchemaVal)
	if err != nil {
		return nil, err
	}

	newTableStruct, err := t.tableStruct.Set(schemaRefKey, schRef)
	if err != nil {
		return nil, err
	}

	return &Table{t.vrw, newTableStruct}, nil
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

// UpdateRows replaces the current row data and returns and updated Table.  Calls to UpdateRows will not be written to the
// database.  The root must be updated with the updated table, and the root must be committed or written.
func (t *Table) UpdateRows(ctx context.Context, updatedRows types.Map) (*Table, error) {
	rowDataRef, err := WriteValAndGetRef(ctx, t.vrw, updatedRows)

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
		return invalid, notFound, tbl, ErrNoConflictsResolved
	}

	conflicts, err := confEdit.Map(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	conflictsRef, err := WriteValAndGetRef(ctx, t.vrw, conflicts)
	if err != nil {
		return nil, nil, nil, err
	}

	updatedSt, err := t.tableStruct.Set(conflictsKey, conflictsRef)
	if err != nil {
		return nil, nil, nil, err
	}

	newTbl := &Table{t.vrw, updatedSt}

	// If we resolved the last conflict, mark the table conflict free
	numRowsInConflict, err := newTbl.NumRowsInConflict(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	if numRowsInConflict == 0 {
		newTbl, err = newTbl.ClearConflicts()
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return invalid, notFound, newTbl, nil
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

// SetIndexData replaces the current internal index map, and returns an updated Table.
func (t *Table) SetIndexData(ctx context.Context, indexesMap types.Map) (*Table, error) {
	indexesRef, err := WriteValAndGetRef(ctx, t.vrw, indexesMap)
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

// SetIndexRowData replaces the current row data for the given index and returns an updated Table.
func (t *Table) SetIndexRowData(ctx context.Context, indexName string, indexRowData types.Map) (*Table, error) {
	indexesMap, err := t.GetIndexData(ctx)
	if err != nil {
		return nil, err
	}

	indexRowDataRef, err := WriteValAndGetRef(ctx, t.vrw, indexRowData)
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
		return nil, ErrNoAutoIncrementValue
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
