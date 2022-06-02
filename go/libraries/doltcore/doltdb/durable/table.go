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
	"bytes"
	"context"
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
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
	GetConflicts(ctx context.Context) (conflict.ConflictSchema, ConflictIndex, error)
	// HasConflicts returns true if this table has conflicts.
	HasConflicts(ctx context.Context) (bool, error)
	// SetConflicts sets the merge conflicts for this table.
	SetConflicts(ctx context.Context, sch conflict.ConflictSchema, conflicts ConflictIndex) (Table, error)
	// ClearConflicts removes all merge conflicts for this table.
	ClearConflicts(ctx context.Context) (Table, error)

	// GetConstraintViolations returns the constraint violations for this table.
	GetConstraintViolations(ctx context.Context) (types.Map, error)
	// SetConstraintViolations sets the constraint violations for this table.
	SetConstraintViolations(ctx context.Context, violations types.Map) (Table, error)

	// GetAutoIncrement returns the AUTO_INCREMENT sequence value for this table.
	GetAutoIncrement(ctx context.Context) (uint64, error)
	// SetAutoIncrement sets the AUTO_INCREMENT sequence value for this table.
	SetAutoIncrement(ctx context.Context, val uint64) (Table, error)

	// DebugString returns the table contents for debugging purposes
	DebugString(ctx context.Context) string
}

type nomsTable struct {
	vrw         types.ValueReadWriter
	tableStruct types.Struct
}

var _ Table = nomsTable{}

var sharePool = pool.NewBuffPool()

// NewNomsTable makes a new Table.
func NewNomsTable(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rows types.Map, indexes IndexSet, autoIncVal types.Value) (Table, error) {
	return NewTable(ctx, vrw, sch, nomsIndex{index: rows, vrw: vrw}, indexes, autoIncVal)
}

// NewTable returns a new Table.
func NewTable(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rows Index, indexes IndexSet, autoIncVal types.Value) (Table, error) {
	if vrw.Format().UsesFlatbuffers() {
		return newDoltDevTable(ctx, vrw, sch, rows, indexes, autoIncVal)
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

	if schema.HasAutoIncrement(sch) && autoIncVal != nil {
		sd[autoIncrementKey] = autoIncVal
	}

	tableStruct, err := types.NewStruct(vrw.Format(), tableStructName, sd)
	if err != nil {
		return nil, err
	}

	return nomsTable{vrw, tableStruct}, nil
}

// TableFromAddr deserializes the table in the chunk at |addr|.
func TableFromAddr(ctx context.Context, vrw types.ValueReadWriter, addr hash.Hash) (Table, error) {
	val, err := vrw.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	if !vrw.Format().UsesFlatbuffers() {
		st, ok := val.(types.Struct)
		if !ok {
			err = errors.New("table ref is unexpected noms value")
			return nil, err
		}

		return nomsTable{vrw: vrw, tableStruct: st}, nil
	} else {
		sm, ok := val.(types.SerialMessage)
		if !ok {
			err = errors.New("table ref is unexpected noms value; not SerialMessage")
			return nil, err
		}
		if serial.GetFileID([]byte(sm)) != serial.TableFileID {
			err = errors.New("table ref is unexpected noms value; GetFileID == " + serial.GetFileID([]byte(sm)))
			return nil, err
		}
		return doltDevTable{vrw, serial.GetRootAsTable([]byte(sm), 0)}, nil
	}
}

// RefFromNomsTable serialized |table|, and returns its types.Ref.
func RefFromNomsTable(ctx context.Context, table Table) (types.Ref, error) {
	nt, ok := table.(nomsTable)
	if ok {
		return refFromNomsValue(ctx, nt.vrw, nt.tableStruct)
	}
	ddt := table.(doltDevTable)

	return refFromNomsValue(ctx, ddt.vrw, ddt.nomsValue())
}

// VrwFromTable returns the types.ValueReadWriter used by |t|.
// todo(andy): this is a temporary method that will be removed when there is a
//  general-purpose abstraction to replace types.ValueReadWriter.
func VrwFromTable(t Table) types.ValueReadWriter {
	if nt, ok := t.(nomsTable); ok {
		return nt.vrw
	} else {
		ddt := t.(doltDevTable)
		return ddt.vrw
	}
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
	return r.(types.Ref).TargetHash(), nil
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

	return indexFromRef(ctx, t.vrw, sch, val.(types.Ref))
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
func (t nomsTable) GetConflicts(ctx context.Context) (conflict.ConflictSchema, ConflictIndex, error) {
	schemasVal, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)
	if err != nil {
		return conflict.ConflictSchema{}, nil, err
	}
	if !ok {
		sch, err := t.GetSchema(ctx)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
		empty, err := NewEmptyConflictIndex(ctx, t.vrw, sch, sch, sch)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
		return conflict.ConflictSchema{}, empty, nil
	}

	schemas, err := conflict.ConflictSchemaFromValue(ctx, t.vrw, schemasVal)
	if err != nil {
		return conflict.ConflictSchema{}, nil, err
	}

	conflictsVal, _, err := t.tableStruct.MaybeGet(conflictsKey)
	if err != nil {
		return conflict.ConflictSchema{}, nil, err
	}

	if conflictsVal == nil {
		confIndex, err := NewEmptyConflictIndex(ctx, t.vrw, schemas.Schema, schemas.MergeSchema, schemas.Base)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
		return conflict.ConflictSchema{}, confIndex, nil
	}

	i, err := conflictIndexFromRef(ctx, t.vrw, schemas.Schema, schemas.MergeSchema, schemas.Base, conflictsVal.(types.Ref))
	if err != nil {
		return conflict.ConflictSchema{}, nil, err
	}

	return schemas, i, nil
}

// SetConflicts implements Table.
func (t nomsTable) SetConflicts(ctx context.Context, schemas conflict.ConflictSchema, conflictData ConflictIndex) (Table, error) {
	conflictsRef, err := RefFromConflictIndex(ctx, t.vrw, conflictData)
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
func (t nomsTable) GetAutoIncrement(ctx context.Context) (uint64, error) {
	val, ok, err := t.tableStruct.MaybeGet(autoIncrementKey)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 1, nil
	}

	// older versions might have serialized auto-increment
	// value as types.Int or types.Float.
	switch t := val.(type) {
	case types.Int:
		return uint64(t), nil
	case types.Uint:
		return uint64(t), nil
	case types.Float:
		return uint64(t), nil
	default:
		return 0, ErrUnknownAutoIncrementValue
	}
}

// SetAutoIncrement implements Table.
func (t nomsTable) SetAutoIncrement(ctx context.Context, val uint64) (Table, error) {
	st, err := t.tableStruct.Set(autoIncrementKey, types.Uint(val))
	if err != nil {
		return nil, err
	}
	return nomsTable{t.vrw, st}, nil
}

func (t nomsTable) DebugString(ctx context.Context) string {
	var buf bytes.Buffer
	err := types.WriteEncodedValue(ctx, &buf, t.tableStruct)
	if err != nil {
		panic(err)
	}

	schemaRefVal, _, _ := t.tableStruct.MaybeGet(schemaRefKey)
	schemaRef := schemaRefVal.(types.Ref)
	schemaVal, err := schemaRef.TargetValue(ctx, t.vrw)
	if err != nil {
		panic(err)
	}

	buf.WriteString("\nschema: ")
	err = types.WriteEncodedValue(ctx, &buf, schemaVal)
	if err != nil {
		panic(err)
	}

	iv, ok, err := t.tableStruct.MaybeGet(indexesKey)
	if err != nil {
		panic(err)
	}

	if ok {
		buf.WriteString("\nindexes: ")
		im, err := iv.(types.Ref).TargetValue(ctx, t.vrw)
		if err != nil {
			panic(err)
		}

		err = types.WriteEncodedValue(ctx, &buf, im)
		if err != nil {
			panic(err)
		}
	}

	buf.WriteString("\ndata:\n")
	data, err := t.GetTableRows(ctx)
	if err != nil {
		panic(err)
	}

	err = types.WriteEncodedValue(ctx, &buf, NomsMapFromIndex(data))
	if err != nil {
		panic(err)
	}

	return buf.String()
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
	return schemaFromAddr(ctx, vrw, ref.TargetHash())
}

func schemaFromAddr(ctx context.Context, vrw types.ValueReadWriter, addr hash.Hash) (schema.Schema, error) {
	schemaVal, err := vrw.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	schema, err := encoding.UnmarshalSchemaNomsValue(ctx, vrw.Format(), schemaVal)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

type doltDevTable struct {
	vrw types.ValueReadWriter
	msg *serial.Table
}

func (t doltDevTable) DebugString(ctx context.Context) string {
	rows, err := t.GetTableRows(ctx)
	if err != nil {
		panic(err)
	}

	if t.vrw.Format() == types.Format_DOLT_DEV {
		m := NomsMapFromIndex(rows)
		var b bytes.Buffer
		_ = types.WriteEncodedValue(ctx, &b, m)
		return b.String()
	} else {
		m := ProllyMapFromIndex(rows)
		var b bytes.Buffer
		m.WalkNodes(ctx, func(ctx context.Context, nd tree.Node) error {
			return tree.OutputProllyNode(&b, nd)
		})
		return b.String()
	}
}

var _ Table = doltDevTable{}

type serialTableFields struct {
	schema            []byte
	rows              []byte
	indexes           prolly.AddressMap
	conflictsdata     []byte
	conflictsours     []byte
	conflictstheirs   []byte
	conflictsancestor []byte
	violations        []byte
	autoincval        uint64
}

func (fields serialTableFields) write() *serial.Table {
	// TODO: Chance for a pool.
	builder := flatbuffers.NewBuilder(1024)

	indexesam := fields.indexes
	indexesbytes := []byte(tree.ValueFromNode(indexesam.Node()).(types.TupleRowStorage))

	schemaoff := builder.CreateByteVector(fields.schema)
	rowsoff := builder.CreateByteVector(fields.rows)
	indexesoff := builder.CreateByteVector(indexesbytes)
	conflictsdataoff := builder.CreateByteVector(fields.conflictsdata)
	conflictsoursoff := builder.CreateByteVector(fields.conflictsours)
	conflictstheirsoff := builder.CreateByteVector(fields.conflictstheirs)
	conflictsbaseoff := builder.CreateByteVector(fields.conflictsancestor)
	serial.ConflictsStart(builder)
	serial.ConflictsAddData(builder, conflictsdataoff)
	serial.ConflictsAddOurSchema(builder, conflictsoursoff)
	serial.ConflictsAddTheirSchema(builder, conflictstheirsoff)
	serial.ConflictsAddAncestorSchema(builder, conflictsbaseoff)
	conflictsoff := serial.ConflictsEnd(builder)

	violationsoff := builder.CreateByteVector(fields.violations)

	serial.TableStart(builder)
	serial.TableAddSchema(builder, schemaoff)
	serial.TableAddPrimaryIndex(builder, rowsoff)
	serial.TableAddSecondaryIndexes(builder, indexesoff)
	serial.TableAddAutoIncrementValue(builder, fields.autoincval)
	serial.TableAddConflicts(builder, conflictsoff)
	serial.TableAddViolations(builder, violationsoff)
	builder.FinishWithFileIdentifier(serial.TableEnd(builder), []byte(serial.TableFileID))
	return serial.GetRootAsTable(builder.FinishedBytes(), 0)
}

func newDoltDevTable(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rows Index, indexes IndexSet, autoIncVal types.Value) (Table, error) {
	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	if err != nil {
		return nil, err
	}

	schemaRef, err := refFromNomsValue(ctx, vrw, schVal)
	if err != nil {
		return nil, err
	}
	schemaAddr := schemaRef.TargetHash()

	rowsbytes, err := rows.bytes()
	if err != nil {
		return nil, err
	}

	if indexes == nil {
		indexes = NewIndexSet(ctx, vrw)
	}

	var autoInc uint64
	if autoIncVal != nil {
		autoInc = uint64(autoIncVal.(types.Uint))
	}

	var emptyhash hash.Hash
	msg := serialTableFields{
		schema:            schemaAddr[:],
		rows:              rowsbytes,
		indexes:           indexes.(doltDevIndexSet).am,
		conflictsdata:     emptyhash[:],
		conflictsours:     emptyhash[:],
		conflictstheirs:   emptyhash[:],
		conflictsancestor: emptyhash[:],
		violations:        emptyhash[:],
		autoincval:        autoInc,
	}.write()

	return doltDevTable{vrw, msg}, nil
}

func (t doltDevTable) nomsValue() types.Value {
	return types.SerialMessage(t.msg.Table().Bytes)
}

func (t doltDevTable) HashOf() (hash.Hash, error) {
	return t.nomsValue().Hash(t.Format())
}

func (t doltDevTable) Format() *types.NomsBinFormat {
	return t.vrw.Format()
}

func (t doltDevTable) GetSchemaHash(ctx context.Context) (hash.Hash, error) {
	return hash.New(t.msg.SchemaBytes()), nil
}

func (t doltDevTable) GetSchema(ctx context.Context) (schema.Schema, error) {
	addr := hash.New(t.msg.SchemaBytes())
	return schemaFromAddr(ctx, t.vrw, addr)
}

func (t doltDevTable) SetSchema(ctx context.Context, sch schema.Schema) (Table, error) {
	newSchemaVal, err := encoding.MarshalSchemaAsNomsValue(ctx, t.vrw, sch)
	if err != nil {
		return nil, err
	}

	schRef, err := refFromNomsValue(ctx, t.vrw, newSchemaVal)
	if err != nil {
		return nil, err
	}

	addr := schRef.TargetHash()
	msg := t.clone()
	copy(msg.SchemaBytes(), addr[:])
	return doltDevTable{t.vrw, msg}, nil
}

func (t doltDevTable) GetTableRows(ctx context.Context) (Index, error) {
	rowbytes := t.msg.PrimaryIndexBytes()
	if t.vrw.Format() == types.Format_DOLT_DEV {
		rowchunk := chunks.NewChunk(rowbytes)
		tv, err := types.DecodeValue(rowchunk, t.vrw)
		if err != nil {
			return nil, err
		}
		return IndexFromNomsMap(tv.(types.Map), t.vrw), nil
	} else {
		sch, err := t.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		m := shim.MapFromValue(types.TupleRowStorage(rowbytes), sch, t.vrw)
		return IndexFromProllyMap(m), nil
	}
}

func (t doltDevTable) SetTableRows(ctx context.Context, rows Index) (Table, error) {
	rowsbytes, err := rows.bytes()
	if err != nil {
		return nil, err
	}

	fields := t.fields()
	fields.rows = rowsbytes
	msg := fields.write()

	return doltDevTable{t.vrw, msg}, nil
}

func (t doltDevTable) GetIndexes(ctx context.Context) (IndexSet, error) {
	ambytes := t.msg.SecondaryIndexesBytes()
	node := tree.NodeFromBytes(ambytes)
	ns := tree.NewNodeStore(shim.ChunkStoreFromVRW(t.vrw))
	return doltDevIndexSet{t.vrw, prolly.NewAddressMap(node, ns)}, nil
}

func (t doltDevTable) SetIndexes(ctx context.Context, indexes IndexSet) (Table, error) {
	fields := t.fields()
	fields.indexes = indexes.(doltDevIndexSet).am
	msg := fields.write()
	return doltDevTable{t.vrw, msg}, nil
}

func (t doltDevTable) GetConflicts(ctx context.Context) (conflict.ConflictSchema, ConflictIndex, error) {
	conflicts := t.msg.Conflicts(nil)

	ouraddr := hash.New(conflicts.OurSchemaBytes())
	theiraddr := hash.New(conflicts.TheirSchemaBytes())
	baseaddr := hash.New(conflicts.AncestorSchemaBytes())

	if ouraddr.IsEmpty() {
		sch, err := t.GetSchema(ctx)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
		empty, err := NewEmptyConflictIndex(ctx, t.vrw, sch, sch, sch)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
		return conflict.ConflictSchema{}, empty, nil
	}

	ourschema, err := getSchemaAtAddr(ctx, t.vrw, ouraddr)
	if err != nil {
		return conflict.ConflictSchema{}, nil, err
	}
	theirschema, err := getSchemaAtAddr(ctx, t.vrw, theiraddr)
	if err != nil {
		return conflict.ConflictSchema{}, nil, err
	}
	baseschema, err := getSchemaAtAddr(ctx, t.vrw, baseaddr)
	if err != nil {
		return conflict.ConflictSchema{}, nil, err
	}

	conflictschema := conflict.ConflictSchema{
		Base:        baseschema,
		Schema:      ourschema,
		MergeSchema: theirschema,
	}

	mapaddr := hash.New(conflicts.DataBytes())
	var conflictIdx ConflictIndex
	if mapaddr.IsEmpty() {
		conflictIdx, err = NewEmptyConflictIndex(ctx, t.vrw, ourschema, theirschema, baseschema)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
	} else {
		conflictIdx, err = conflictIndexFromAddr(ctx, t.vrw, ourschema, theirschema, baseschema, mapaddr)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
	}

	return conflictschema, conflictIdx, nil
}

func (t doltDevTable) HasConflicts(ctx context.Context) (bool, error) {
	conflicts := t.msg.Conflicts(nil)
	addr := hash.New(conflicts.OurSchemaBytes())
	return !addr.IsEmpty(), nil
}

func (t doltDevTable) SetConflicts(ctx context.Context, sch conflict.ConflictSchema, conflicts ConflictIndex) (Table, error) {
	conflictsRef, err := RefFromConflictIndex(ctx, t.vrw, conflicts)
	if err != nil {
		return nil, err
	}
	conflictsAddr := conflictsRef.TargetHash()

	baseaddr, err := getAddrForSchema(ctx, t.vrw, sch.Base)
	if err != nil {
		return nil, err
	}
	ouraddr, err := getAddrForSchema(ctx, t.vrw, sch.Schema)
	if err != nil {
		return nil, err
	}
	theiraddr, err := getAddrForSchema(ctx, t.vrw, sch.MergeSchema)
	if err != nil {
		return nil, err
	}

	msg := t.clone()
	cmsg := msg.Conflicts(nil)
	copy(cmsg.DataBytes(), conflictsAddr[:])
	copy(cmsg.OurSchemaBytes(), ouraddr[:])
	copy(cmsg.TheirSchemaBytes(), theiraddr[:])
	copy(cmsg.AncestorSchemaBytes(), baseaddr[:])

	return doltDevTable{t.vrw, msg}, nil
}

func (t doltDevTable) ClearConflicts(ctx context.Context) (Table, error) {
	msg := t.clone()
	conflicts := msg.Conflicts(nil)
	var emptyhash hash.Hash
	copy(conflicts.DataBytes(), emptyhash[:])
	copy(conflicts.OurSchemaBytes(), emptyhash[:])
	copy(conflicts.TheirSchemaBytes(), emptyhash[:])
	copy(conflicts.AncestorSchemaBytes(), emptyhash[:])
	return doltDevTable{t.vrw, msg}, nil
}

func (t doltDevTable) GetConstraintViolations(ctx context.Context) (types.Map, error) {
	addr := hash.New(t.msg.ViolationsBytes())
	if addr.IsEmpty() {
		return types.NewMap(ctx, t.vrw)
	}
	v, err := t.vrw.ReadValue(ctx, addr)
	if err != nil {
		return types.Map{}, err
	}
	return v.(types.Map), nil
}

func (t doltDevTable) SetConstraintViolations(ctx context.Context, violations types.Map) (Table, error) {
	var addr hash.Hash
	if violations != types.EmptyMap && violations.Len() != 0 {
		ref, err := refFromNomsValue(ctx, t.vrw, violations)
		if err != nil {
			return nil, err
		}
		addr = ref.TargetHash()
	}
	msg := t.clone()
	copy(msg.ViolationsBytes(), addr[:])
	return doltDevTable{t.vrw, msg}, nil
}

func (t doltDevTable) GetAutoIncrement(ctx context.Context) (uint64, error) {
	res := t.msg.AutoIncrementValue()
	if res == 0 {
		return 1, nil
	}
	return res, nil
}

func (t doltDevTable) SetAutoIncrement(ctx context.Context, val uint64) (Table, error) {
	// TODO: This clones before checking if the mutate will work.
	msg := t.clone()
	if !msg.MutateAutoIncrementValue(val) {
		fields := t.fields()
		fields.autoincval = val
		msg = fields.write()
	}
	return doltDevTable{t.vrw, msg}, nil
}

func (t doltDevTable) clone() *serial.Table {
	bs := make([]byte, len(t.msg.Table().Bytes))
	copy(bs, t.msg.Table().Bytes)
	var ret serial.Table
	ret.Init(bs, t.msg.Table().Pos)
	return &ret
}

func (t doltDevTable) fields() serialTableFields {
	ambytes := t.msg.SecondaryIndexesBytes()
	node := tree.NodeFromBytes(ambytes)
	ns := tree.NewNodeStore(shim.ChunkStoreFromVRW(t.vrw))

	conflicts := t.msg.Conflicts(nil)
	return serialTableFields{
		schema:            t.msg.SchemaBytes(),
		rows:              t.msg.PrimaryIndexBytes(),
		indexes:           prolly.NewAddressMap(node, ns),
		conflictsdata:     conflicts.DataBytes(),
		conflictsours:     conflicts.OurSchemaBytes(),
		conflictstheirs:   conflicts.TheirSchemaBytes(),
		conflictsancestor: conflicts.AncestorSchemaBytes(),
		violations:        t.msg.ViolationsBytes(),
		autoincval:        t.msg.AutoIncrementValue(),
	}
}

func getSchemaAtAddr(ctx context.Context, vrw types.ValueReadWriter, addr hash.Hash) (schema.Schema, error) {
	val, err := vrw.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}
	return encoding.UnmarshalSchemaNomsValue(ctx, vrw.Format(), val)
}

func getAddrForSchema(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (hash.Hash, error) {
	st, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	if err != nil {
		return hash.Hash{}, err
	}
	ref, err := vrw.WriteValue(ctx, st)
	if err != nil {
		return hash.Hash{}, err
	}
	return ref.TargetHash(), nil
}
