// Copyright 2021 Dolthub, Inc.
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

package durable

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
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
)

var (
	ErrNoConflictsResolved  = errors.New("no conflicts resolved")
	ErrNoAutoIncrementValue = fmt.Errorf("auto increment set for non-numeric column type")
)

func VrwFromTable(t Table) types.ValueReadWriter {
	return t.(nomsTable).vrw
}

type Table interface {
	HashOf() (hash.Hash, error)

	GetSchemaHash(ctx context.Context) (hash.Hash, error)
	GetSchema(ctx context.Context) (schema.Schema, error)
	SetSchema(ctx context.Context, sch schema.Schema) (Table, error)

	GetTableRows(ctx context.Context) (types.Map, error)
	SetTableRows(ctx context.Context, rows types.Map) (Table, error)

	GetIndexes(ctx context.Context) (types.Map, error)
	SetIndexes(ctx context.Context, indexes types.Map) (Table, error)

	GetConflicts(ctx context.Context) (conflict.ConflictSchema, types.Map, error)
	HasConflicts(ctx context.Context) (bool, error)
	SetConflicts(ctx context.Context, sch conflict.ConflictSchema, conflicts types.Map) (Table, error)
	ClearConflicts(ctx context.Context) (Table, error)

	GetConstraintViolations(ctx context.Context) (types.Map, error)
	SetConstraintViolations(ctx context.Context, violations types.Map) (Table, error)

	GetAutoIncrement(ctx context.Context) (types.Value, error)
	SetAutoIncrement(ctx context.Context, val types.Value) (Table, error)
}

type nomsTable struct {
	vrw         types.ValueReadWriter
	tableStruct types.Struct
}

var _ Table = nomsTable{}

func NewNomsTable(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rowData types.Map, indexData types.Map, autoIncVal types.Value) (Table, error) {
	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	if err != nil {
		return nil, err
	}

	schemaRef, err := refFromNomsValue(ctx, vrw, schVal)
	if err != nil {
		return nil, err
	}

	rowDataRef, err := refFromNomsValue(ctx, vrw, rowData)
	if err != nil {
		return nil, err
	}

	indexesRef, err := refFromNomsValue(ctx, vrw, indexData)
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

	return nomsTable{vrw, tableStruct}, nil
}

func NomsTableFromRef(ctx context.Context, vrw types.ValueReadWriter, ref types.Ref) (Table, error) {
	val, err := ref.TargetValue(ctx, vrw)
	if err != nil {
		return nil, err
	}

	st, ok := val.(types.Struct)
	if !ok {
		err = errors.New("table ref is unexpected noms value")
		return nil, err
	}

	return nomsTable{vrw: vrw, tableStruct: st}, nil
}

func RefFromNomsTable(ctx context.Context, table Table) (types.Ref, error) {
	nt := table.(nomsTable)
	return refFromNomsValue(ctx, nt.vrw, nt.tableStruct)
}

func (t nomsTable) Format() *types.NomsBinFormat {
	return t.vrw.Format()
}

// ValueReadWriter returns the ValueReadWriter for this table.
func (t nomsTable) ValueReadWriter() types.ValueReadWriter {
	return t.vrw
}

func (t nomsTable) HashOf() (hash.Hash, error) {
	return t.tableStruct.Hash(t.vrw.Format())
}

// GetSchema will retrieve the schema being referenced from the table in noms and unmarshal it.
func (t nomsTable) GetSchema(ctx context.Context) (schema.Schema, error) {
	schemaRefVal, _, err := t.tableStruct.MaybeGet(schemaRefKey)

	if err != nil {
		return nil, err
	}

	schemaRef := schemaRefVal.(types.Ref)
	return schemaFromRef(ctx, t.vrw, schemaRef)
}

func (t nomsTable) GetSchemaHash(ctx context.Context) (hash.Hash, error) {
	r, _, err := t.tableStruct.MaybeGet(schemaRefKey)
	if err != nil {
		return hash.Hash{}, err
	}
	return r.Hash(t.vrw.Format())
}

// UpdateSchema updates the table with the schema given and returns the updated table. The original table is unchanged.
func (t nomsTable) SetSchema(ctx context.Context, sch schema.Schema) (Table, error) {
	newSchemaVal, err := encoding.MarshalSchemaAsNomsValue(ctx, t.vrw, sch)
	if err != nil {
		return nil, err
	}

	schRef, err := refFromNomsValue(ctx, t.vrw, newSchemaVal)
	if err != nil {
		return nil, err
	}

	newTableStruct, err := t.tableStruct.Set(schemaRefKey, schRef)
	if err != nil {
		return nil, err
	}

	return nomsTable{t.vrw, newTableStruct}, nil
}

// UpdateRows replaces the current row data and returns and updated Table.  Calls to UpdateRows will not be written to the
// database.  The root must be updated with the updated table, and the root must be committed or written.
func (t nomsTable) SetTableRows(ctx context.Context, updatedRows types.Map) (Table, error) {
	rowDataRef, err := refFromNomsValue(ctx, t.vrw, updatedRows)

	if err != nil {
		return nil, err
	}

	updatedSt, err := t.tableStruct.Set(tableRowsKey, rowDataRef)

	if err != nil {
		return nil, err
	}

	return nomsTable{t.vrw, updatedSt}, nil
}

// GetRowData retrieves the underlying map which is a map from a primary key to a list of field values.
func (t nomsTable) GetTableRows(ctx context.Context) (types.Map, error) {
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

// GetIndexData returns the internal index map which goes from index name to a ref of the row data map.
func (t nomsTable) GetIndexes(ctx context.Context) (types.Map, error) {
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
func (t nomsTable) SetIndexes(ctx context.Context, indexesMap types.Map) (Table, error) {
	indexesRef, err := refFromNomsValue(ctx, t.vrw, indexesMap)
	if err != nil {
		return nil, err
	}

	newTableStruct, err := t.tableStruct.Set(indexesKey, indexesRef)
	if err != nil {
		return nil, err
	}

	return nomsTable{t.vrw, newTableStruct}, nil
}

func (t nomsTable) HasConflicts(ctx context.Context) (bool, error) {
	_, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)
	return ok, err
}

// GetConflicts returns a map built from ValueReadWriter when there are no conflicts in table
func (t nomsTable) GetConflicts(ctx context.Context) (conflict.ConflictSchema, types.Map, error) {
	schemasVal, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)
	if err != nil {
		return conflict.ConflictSchema{}, types.EmptyMap, err
	}
	if !ok {
		confMap, _ := types.NewMap(ctx, t.ValueReadWriter())
		return conflict.ConflictSchema{}, confMap, nil
	}

	schemas, err := conflict.ConflictSchemaFromValue(ctx, t.vrw, schemasVal)
	if err != nil {
		return conflict.ConflictSchema{}, types.EmptyMap, err
	}

	conflictsVal, _, err := t.tableStruct.MaybeGet(conflictsKey)
	if err != nil {
		return conflict.ConflictSchema{}, types.EmptyMap, err
	}

	confMap := types.EmptyMap
	if conflictsVal != nil {
		confMapRef := conflictsVal.(types.Ref)
		v, err := confMapRef.TargetValue(ctx, t.vrw)

		if err != nil {
			return conflict.ConflictSchema{}, types.EmptyMap, err
		}

		confMap = v.(types.Map)
	}

	return schemas, confMap, nil
}

func (t nomsTable) SetConflicts(ctx context.Context, schemas conflict.ConflictSchema, conflictData types.Map) (Table, error) {
	conflictsRef, err := refFromNomsValue(ctx, t.vrw, conflictData)
	if err != nil {
		return nil, err
	}

	tpl, err := conflict.ValueFromConflictSchema(ctx, t.vrw, schemas)
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

	return nomsTable{t.vrw, updatedSt}, nil
}

func (t nomsTable) GetConflictSchemas(ctx context.Context) (base, sch, mergeSch schema.Schema, err error) {
	schemasVal, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)

	if err != nil {
		return nil, nil, nil, err
	}

	if ok {
		schemas, err := conflict.ConflictFromTuple(schemasVal.(types.Tuple))

		if err != nil {
			return nil, nil, nil, err
		}

		baseRef := schemas.Base.(types.Ref)
		valRef := schemas.Value.(types.Ref)
		mergeRef := schemas.MergeValue.(types.Ref)

		var baseSch, sch, mergeSch schema.Schema
		if baseSch, err = schemaFromRef(ctx, t.vrw, baseRef); err == nil {
			if sch, err = schemaFromRef(ctx, t.vrw, valRef); err == nil {
				mergeSch, err = schemaFromRef(ctx, t.vrw, mergeRef)
			}
		}

		return baseSch, sch, mergeSch, err
	}
	return nil, nil, nil, nil
}

func (t nomsTable) ClearConflicts(ctx context.Context) (Table, error) {
	tSt, err := t.tableStruct.Delete(conflictSchemasKey)

	if err != nil {
		return nil, err
	}

	tSt, err = tSt.Delete(conflictsKey)

	if err != nil {
		return nil, err
	}

	return nomsTable{t.vrw, tSt}, nil
}

// GetConstraintViolations returns a map of all constraint violations for this table, along with a bool indicating
// whether the table has any violations.
func (t nomsTable) GetConstraintViolations(ctx context.Context) (types.Map, error) {
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
func (t nomsTable) SetConstraintViolations(ctx context.Context, violationsMap types.Map) (Table, error) {
	// We can't just call violationsMap.Empty() as we can't guarantee that the caller passed in an instantiated map
	if violationsMap == types.EmptyMap || violationsMap.Len() == 0 {
		updatedStruct, err := t.tableStruct.Delete(constraintViolationsKey)
		if err != nil {
			return nil, err
		}
		return nomsTable{t.vrw, updatedStruct}, nil
	}
	constraintViolationsRef, err := refFromNomsValue(ctx, t.vrw, violationsMap)
	if err != nil {
		return nil, err
	}
	updatedStruct, err := t.tableStruct.Set(constraintViolationsKey, constraintViolationsRef)
	if err != nil {
		return nil, err
	}
	return nomsTable{t.vrw, updatedStruct}, nil
}

func (t nomsTable) GetAutoIncrement(ctx context.Context) (types.Value, error) {
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

func (t nomsTable) SetAutoIncrement(ctx context.Context, val types.Value) (Table, error) {
	switch val.(type) {
	case types.Int, types.Uint, types.Float:
		st, err := t.tableStruct.Set(autoIncrementKey, val)
		if err != nil {
			return nil, err
		}
		return nomsTable{t.vrw, st}, nil

	default:
		return nil, fmt.Errorf("cannot set auto increment to non-numeric value")
	}
}

func refFromNomsValue(ctx context.Context, vrw types.ValueReadWriter, val types.Value) (types.Ref, error) {
	valRef, err := types.NewRef(val, vrw.Format())

	if err != nil {
		return types.Ref{}, err
	}

	targetVal, err := valRef.TargetValue(ctx, vrw)

	if err != nil {
		return types.Ref{}, err
	}

	if targetVal == nil {
		_, err = vrw.WriteValue(ctx, val)

		if err != nil {
			return types.Ref{}, err
		}
	}

	return valRef, err
}

func schemaFromRef(ctx context.Context, vrw types.ValueReadWriter, ref types.Ref) (schema.Schema, error) {
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
