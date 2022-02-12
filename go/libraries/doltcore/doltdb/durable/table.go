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
	ErrUnknownAutoIncrementValue = fmt.Errorf("auto increment set for non-numeric column type")
)

var (
	errNbfUnkown      = fmt.Errorf("unknown NomsBinFormat")
	errNbfUnsupported = fmt.Errorf("operation unsupported for NomsBinFormat")
)

// Table is a Dolt table that can be persisted.
type Table interface {
	// HashOf returns the hash.Hash of this table.
	HashOf() (hash.Hash, error)

	// Format returns the types.NomsBinFormat for this table.
	Format() *types.NomsBinFormat

	// GetSchemaHash returns the hash.Hash of this table's schema.
	GetSchemaHash(ctx context.Context) (hash.Hash, error)
	// GetSchema returns this table's schema.
	GetSchema(ctx context.Context) (schema.Schema, error)
	// SetSchema sets this table's schema.
	SetSchema(ctx context.Context, sch schema.Schema) (Table, error)

	// GetTableRows returns this tables rows.
	GetTableRows(ctx context.Context) (Index, error)
	// SetTableRows sets this table's rows.
	SetTableRows(ctx context.Context, rows Index) (Table, error)

	// GetIndexes returns the secondary indexes for this table.
	GetIndexes(ctx context.Context) (IndexSet, error)
	// SetIndexes sets the secondary indexes for this table.
	SetIndexes(ctx context.Context, indexes IndexSet) (Table, error)

	// GetConflicts returns the merge conflicts for this table.
	GetConflicts(ctx context.Context) (conflict.ConflictSchema, types.Map, error)
	// HasConflicts returns true if this table has conflicts.
	HasConflicts(ctx context.Context) (bool, error)
	// SetConflicts sets the merge conflicts for this table.
	SetConflicts(ctx context.Context, sch conflict.ConflictSchema, conflicts types.Map) (Table, error)
	// ClearConflicts removes all merge conflicts for this table.
	ClearConflicts(ctx context.Context) (Table, error)

	// GetConstraintViolations returns the constraint violations for this table.
	GetConstraintViolations(ctx context.Context) (types.Map, error)
	// SetConstraintViolations sets the constraint violations for this table.
	SetConstraintViolations(ctx context.Context, violations types.Map) (Table, error)

	// GetAutoIncrement returns the AUTO_INCREMENT sequence value for this table.
	GetAutoIncrement(ctx context.Context) (types.Value, error)
	// SetAutoIncrement sets the AUTO_INCREMENT sequence value for this table.
	SetAutoIncrement(ctx context.Context, val types.Value) (Table, error)
}

type nomsTable struct {
	vrw         types.ValueReadWriter
	tableStruct types.Struct
}

var _ Table = nomsTable{}

// NewNomsTable makes a new Table.
func NewNomsTable(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rows types.Map, indexes IndexSet, autoIncVal types.Value) (Table, error) {
	return NewTable(ctx, vrw, sch, nomsIndex{index: rows, vrw: vrw}, indexes, autoIncVal)
}

// NewTable returns a new Table.
func NewTable(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rows Index, indexes IndexSet, autoIncVal types.Value) (Table, error) {
	if types.IsFormat_DOLT_1(vrw.Format()) && schema.IsKeyless(sch) {
		return nil, errNbfUnsupported
	}

	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	if err != nil {
		return nil, err
	}

	schemaRef, err := refFromNomsValue(ctx, vrw, schVal)
	if err != nil {
		return nil, err
	}

	rowsRef, err := RefFromIndex(ctx, vrw, rows)
	if err != nil {
		return nil, err
	}

	if indexes == nil {
		indexes = NewIndexSet(ctx, vrw)
	}

	indexesRef, err := refFromNomsValue(ctx, vrw, mapFromIndexSet(indexes))
	if err != nil {
		return nil, err
	}

	sd := types.StructData{
		schemaRefKey: schemaRef,
		tableRowsKey: rowsRef,
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

// NomsTableFromRef deserializes the table referenced by |ref|.
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

// RefFromNomsTable serialized |table|, and returns its types.Ref.
func RefFromNomsTable(ctx context.Context, table Table) (types.Ref, error) {
	nt := table.(nomsTable)
	return refFromNomsValue(ctx, nt.vrw, nt.tableStruct)
}

// VrwFromTable returns the types.ValueReadWriter used by |t|.
// todo(andy): this is a temporary method that will be removed when there is a
//  general-purpose abstraction to replace types.ValueReadWriter.
func VrwFromTable(t Table) types.ValueReadWriter {
	return t.(nomsTable).vrw
}

// valueReadWriter returns the valueReadWriter for this table.
func (t nomsTable) valueReadWriter() types.ValueReadWriter {
	return t.vrw
}

// HashOf implements Table.
func (t nomsTable) HashOf() (hash.Hash, error) {
	return t.tableStruct.Hash(t.vrw.Format())
}

// Format returns the types.NomsBinFormat for this index.
func (t nomsTable) Format() *types.NomsBinFormat {
	return t.vrw.Format()
}

// GetSchema implements Table.
func (t nomsTable) GetSchema(ctx context.Context) (schema.Schema, error) {
	schemaRefVal, _, err := t.tableStruct.MaybeGet(schemaRefKey)

	if err != nil {
		return nil, err
	}

	schemaRef := schemaRefVal.(types.Ref)
	return schemaFromRef(ctx, t.vrw, schemaRef)
}

// GetSchemaHash implements Table.
func (t nomsTable) GetSchemaHash(ctx context.Context) (hash.Hash, error) {
	r, _, err := t.tableStruct.MaybeGet(schemaRefKey)
	if err != nil {
		return hash.Hash{}, err
	}
	return r.Hash(t.vrw.Format())
}

// SetSchema implements Table.
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

// SetTableRows implements Table.
func (t nomsTable) SetTableRows(ctx context.Context, updatedRows Index) (Table, error) {
	rowsRef, err := RefFromIndex(ctx, t.vrw, updatedRows)
	if err != nil {
		return nil, err
	}

	updatedSt, err := t.tableStruct.Set(tableRowsKey, rowsRef)
	if err != nil {
		return nil, err
	}

	return nomsTable{t.vrw, updatedSt}, nil
}

// GetTableRows implements Table.
func (t nomsTable) GetTableRows(ctx context.Context) (Index, error) {
	val, _, err := t.tableStruct.MaybeGet(tableRowsKey)
	if err != nil {
		return nil, err
	}

	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	return IndexFromRef(ctx, t.vrw, sch, val.(types.Ref))
}

// GetIndexes implements Table.
func (t nomsTable) GetIndexes(ctx context.Context) (IndexSet, error) {
	iv, ok, err := t.tableStruct.MaybeGet(indexesKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return NewIndexSet(ctx, t.vrw), nil
	}

	im, err := iv.(types.Ref).TargetValue(ctx, t.vrw)
	if err != nil {
		return nil, err
	}

	return nomsIndexSet{
		indexes: im.(types.Map),
		vrw:     t.vrw,
	}, nil
}

// SetIndexes implements Table.
func (t nomsTable) SetIndexes(ctx context.Context, indexes IndexSet) (Table, error) {
	if indexes == nil {
		indexes = NewIndexSet(ctx, t.vrw)
	}

	indexesRef, err := refFromNomsValue(ctx, t.vrw, mapFromIndexSet(indexes))
	if err != nil {
		return nil, err
	}

	newTableStruct, err := t.tableStruct.Set(indexesKey, indexesRef)
	if err != nil {
		return nil, err
	}

	return nomsTable{t.vrw, newTableStruct}, nil
}

// HasConflicts implements Table.
func (t nomsTable) HasConflicts(ctx context.Context) (bool, error) {
	_, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)
	return ok, err
}

// GetConflicts implements Table.
func (t nomsTable) GetConflicts(ctx context.Context) (conflict.ConflictSchema, types.Map, error) {
	schemasVal, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)
	if err != nil {
		return conflict.ConflictSchema{}, types.EmptyMap, err
	}
	if !ok {
		confMap, _ := types.NewMap(ctx, t.valueReadWriter())
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

// SetConflicts implements Table.
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

// GetConflictSchemas implements Table.
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

// ClearConflicts implements Table.
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

// GetConstraintViolations implements Table.
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

// SetConstraintViolations implements Table.
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

// GetAutoIncrement implements Table.
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
		return nil, ErrUnknownAutoIncrementValue
	}
}

// SetAutoIncrement implements Table.
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
