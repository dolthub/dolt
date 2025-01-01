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

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	tableStructName = "table"

	schemaRefKey            = "schema_ref"
	tableRowsKey            = "rows"
	artifactsKey            = "artifacts"
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
	errNbfUnknown     = fmt.Errorf("unknown NomsBinFormat")
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

	// GetTableRows returns this table's rows.
	GetTableRows(ctx context.Context) (Index, error)
	// GetTableRowsWithDescriptors returns this table's rows with fewer deserialization calls
	GetTableRowsWithDescriptors(ctx context.Context, kd, vd val.TupleDesc) (Index, error)
	// SetTableRows sets this table's rows.
	SetTableRows(ctx context.Context, rows Index) (Table, error)

	// GetIndexes returns the secondary indexes for this table.
	GetIndexes(ctx context.Context) (IndexSet, error)
	// SetIndexes sets the secondary indexes for this table.
	SetIndexes(ctx context.Context, indexes IndexSet) (Table, error)

	// GetArtifacts returns the merge artifacts for this table.
	GetArtifacts(ctx context.Context) (ArtifactIndex, error)
	// SetArtifacts sets the merge artifacts for this table.
	SetArtifacts(ctx context.Context, artifacts ArtifactIndex) (Table, error)

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
	DebugString(ctx context.Context, ns tree.NodeStore) string
}

type nomsTable struct {
	vrw         types.ValueReadWriter
	ns          tree.NodeStore
	tableStruct types.Struct
}

var _ Table = nomsTable{}

var sharePool = pool.NewBuffPool()

// NewNomsTable makes a new Table.
func NewNomsTable(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema, rows types.Map, indexes IndexSet, autoIncVal types.Value) (Table, error) {
	return NewTable(ctx, vrw, ns, sch, nomsIndex{index: rows, vrw: vrw}, indexes, autoIncVal)
}

// NewTable returns a new Table.
func NewTable(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema, rows Index, indexes IndexSet, autoIncVal types.Value) (Table, error) {
	if vrw.Format().UsesFlatbuffers() {
		return newDoltDevTable(ctx, vrw, ns, sch, rows, indexes, autoIncVal)
	}

	schVal, err := encoding.MarshalSchema(ctx, vrw, sch)
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
		indexes, err = NewIndexSet(ctx, vrw, ns)
		if err != nil {
			return nil, err
		}
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

	return nomsTable{vrw, ns, tableStruct}, nil
}

// TableFromAddr deserializes the table in the chunk at |addr|.
func TableFromAddr(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, addr hash.Hash) (Table, error) {
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

		return nomsTable{vrw: vrw, tableStruct: st, ns: ns}, nil
	} else {
		sm, ok := val.(types.SerialMessage)
		if !ok {
			err = errors.New("table ref is unexpected noms value; not SerialMessage")
			return nil, err
		}
		id := serial.GetFileID(sm)
		if id != serial.TableFileID {
			err = errors.New("table ref is unexpected noms value; GetFileID == " + id)
			return nil, err
		}
		st, err := serial.TryGetRootAsTable([]byte(sm), serial.MessagePrefixSz)
		if err != nil {
			return nil, err
		}
		return doltDevTable{vrw, ns, st}, nil
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
// general-purpose abstraction to replace types.ValueReadWriter.
func VrwFromTable(t Table) types.ValueReadWriter {
	if nt, ok := t.(nomsTable); ok {
		return nt.vrw
	} else {
		ddt := t.(doltDevTable)
		return ddt.vrw
	}
}

func NodeStoreFromTable(t Table) tree.NodeStore {
	if nt, ok := t.(nomsTable); ok {
		return nt.ns
	} else {
		ddt := t.(doltDevTable)
		return ddt.ns
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
	newSchemaVal, err := encoding.MarshalSchema(ctx, t.vrw, sch)
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

	return nomsTable{t.vrw, t.ns, newTableStruct}, nil
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

	return nomsTable{t.vrw, t.ns, updatedSt}, nil
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

	return indexFromRef(ctx, t.vrw, t.ns, sch, val.(types.Ref))
}

func (t nomsTable) GetTableRowsWithDescriptors(ctx context.Context, kd, vd val.TupleDesc) (Index, error) {
	return nil, fmt.Errorf("nomsTable does not implement GetTableRowsWithDescriptors")
}

// GetIndexes implements Table.
func (t nomsTable) GetIndexes(ctx context.Context) (IndexSet, error) {
	iv, ok, err := t.tableStruct.MaybeGet(indexesKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return NewIndexSet(ctx, t.vrw, t.ns)
	}

	im, err := iv.(types.Ref).TargetValue(ctx, t.vrw)
	if err != nil {
		return nil, err
	}

	return nomsIndexSet{
		indexes: im.(types.Map),
		vrw:     t.vrw,
		ns:      t.ns,
	}, nil
}

// SetIndexes implements Table.
func (t nomsTable) SetIndexes(ctx context.Context, indexes IndexSet) (Table, error) {
	if indexes == nil {
		var err error
		indexes, err = NewIndexSet(ctx, t.vrw, t.ns)
		if err != nil {
			return nil, err
		}
	}

	indexesRef, err := refFromNomsValue(ctx, t.vrw, mapFromIndexSet(indexes))
	if err != nil {
		return nil, err
	}

	newTableStruct, err := t.tableStruct.Set(indexesKey, indexesRef)
	if err != nil {
		return nil, err
	}

	return nomsTable{t.vrw, t.ns, newTableStruct}, nil
}

// GetArtifacts implements Table.
func (t nomsTable) GetArtifacts(ctx context.Context) (ArtifactIndex, error) {
	if t.Format() != types.Format_DOLT {
		panic("artifacts not implemented for old storage format")
	}

	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	val, ok, err := t.tableStruct.MaybeGet(artifactsKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return NewEmptyArtifactIndex(ctx, t.vrw, t.ns, sch)
	}

	return artifactIndexFromRef(ctx, t.vrw, t.ns, sch, val.(types.Ref))
}

// SetArtifacts implements Table.
func (t nomsTable) SetArtifacts(ctx context.Context, artifacts ArtifactIndex) (Table, error) {
	if t.Format() != types.Format_DOLT {
		panic("artifacts not implemented for old storage format")
	}

	ref, err := RefFromArtifactIndex(ctx, t.vrw, artifacts)
	if err != nil {
		return nil, err
	}

	updated, err := t.tableStruct.Set(artifactsKey, ref)
	if err != nil {
		return nil, err
	}

	return nomsTable{t.vrw, t.ns, updated}, nil
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
		empty, err := NewEmptyConflictIndex(ctx, t.vrw, t.ns, sch, sch, sch)
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
		confIndex, err := NewEmptyConflictIndex(ctx, t.vrw, t.ns, schemas.Schema, schemas.MergeSchema, schemas.Base)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
		return conflict.ConflictSchema{}, confIndex, nil
	}

	i, err := conflictIndexFromRef(ctx, t.vrw, t.ns, schemas.Schema, schemas.MergeSchema, schemas.Base, conflictsVal.(types.Ref))
	if err != nil {
		return conflict.ConflictSchema{}, nil, err
	}

	return schemas, i, nil
}

// SetConflicts implements Table.
func (t nomsTable) SetConflicts(ctx context.Context, schemas conflict.ConflictSchema, conflictData ConflictIndex) (Table, error) {
	if t.Format() == types.Format_DOLT {
		panic("should use artifacts")
	}

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

	return nomsTable{t.vrw, t.ns, updatedSt}, nil
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
	if t.Format() == types.Format_DOLT {
		panic("should use artifacts")
	}

	tSt, err := t.tableStruct.Delete(conflictSchemasKey)

	if err != nil {
		return nil, err
	}

	tSt, err = tSt.Delete(conflictsKey)

	if err != nil {
		return nil, err
	}

	return nomsTable{t.vrw, t.ns, tSt}, nil
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
		return nomsTable{t.vrw, t.ns, updatedStruct}, nil
	}
	constraintViolationsRef, err := refFromNomsValue(ctx, t.vrw, violationsMap)
	if err != nil {
		return nil, err
	}
	updatedStruct, err := t.tableStruct.Set(constraintViolationsKey, constraintViolationsRef)
	if err != nil {
		return nil, err
	}
	return nomsTable{t.vrw, t.ns, updatedStruct}, nil
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
	return nomsTable{t.vrw, t.ns, st}, nil
}

func (t nomsTable) DebugString(ctx context.Context, ns tree.NodeStore) string {
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
	return vrw.WriteValue(ctx, val)
}

func schemaFromRef(ctx context.Context, vrw types.ValueReadWriter, ref types.Ref) (schema.Schema, error) {
	return schemaFromAddr(ctx, vrw, ref.TargetHash())
}

func schemaFromAddr(ctx context.Context, vrw types.ValueReadWriter, addr hash.Hash) (schema.Schema, error) {
	return encoding.UnmarshalSchemaAtAddr(ctx, vrw, addr)
}

type doltDevTable struct {
	vrw types.ValueReadWriter
	ns  tree.NodeStore
	msg *serial.Table
}

func (t doltDevTable) DebugString(ctx context.Context, ns tree.NodeStore) string {
	rows, err := t.GetTableRows(ctx)
	if err != nil {
		panic(err)
	}

	schema, err := t.GetSchema(ctx)
	if err != nil {
		panic(err)
	}

	return rows.DebugString(ctx, ns, schema)
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
	artifacts         []byte
	autoincval        uint64
}

func (fields serialTableFields) write() (*serial.Table, error) {
	// TODO: Chance for a pool.
	builder := flatbuffers.NewBuilder(1024)

	indexesam := fields.indexes
	indexesbytes := []byte(tree.ValueFromNode(indexesam.Node()).(types.SerialMessage))

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
	artifactsoff := builder.CreateByteVector(fields.artifacts)

	serial.TableStart(builder)
	serial.TableAddSchema(builder, schemaoff)
	serial.TableAddPrimaryIndex(builder, rowsoff)
	serial.TableAddSecondaryIndexes(builder, indexesoff)
	serial.TableAddAutoIncrementValue(builder, fields.autoincval)
	serial.TableAddConflicts(builder, conflictsoff)
	serial.TableAddViolations(builder, violationsoff)
	serial.TableAddArtifacts(builder, artifactsoff)
	bs := serial.FinishMessage(builder, serial.TableEnd(builder), []byte(serial.TableFileID))
	return serial.TryGetRootAsTable(bs, serial.MessagePrefixSz)
}

func newDoltDevTable(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema, rows Index, indexes IndexSet, autoIncVal types.Value) (Table, error) {
	schVal, err := encoding.MarshalSchema(ctx, vrw, sch)
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
		indexes, err = NewIndexSet(ctx, vrw, ns)
		if err != nil {
			return nil, err
		}
	}

	var autoInc uint64
	if autoIncVal != nil {
		autoInc = uint64(autoIncVal.(types.Uint))
	}

	var emptyhash hash.Hash
	msg, err := serialTableFields{
		schema:            schemaAddr[:],
		rows:              rowsbytes,
		indexes:           indexes.(doltDevIndexSet).am,
		conflictsdata:     emptyhash[:],
		conflictsours:     emptyhash[:],
		conflictstheirs:   emptyhash[:],
		conflictsancestor: emptyhash[:],
		violations:        emptyhash[:],
		artifacts:         emptyhash[:],
		autoincval:        autoInc,
	}.write()
	if err != nil {
		return nil, err
	}

	return doltDevTable{vrw, ns, msg}, nil
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
	newSchemaVal, err := encoding.MarshalSchema(ctx, t.vrw, sch)
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
	return doltDevTable{t.vrw, t.ns, msg}, nil
}

func (t doltDevTable) GetTableRows(ctx context.Context) (Index, error) {
	rowbytes := t.msg.PrimaryIndexBytes()
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	m, err := shim.MapInterfaceFromValue(ctx, types.SerialMessage(rowbytes), sch, t.ns, false)
	if err != nil {
		return nil, err
	}
	return IndexFromMapInterface(m), nil
}

func (t doltDevTable) GetTableRowsWithDescriptors(ctx context.Context, kd, vd val.TupleDesc) (Index, error) {
	rowbytes := t.msg.PrimaryIndexBytes()
	m, err := shim.MapFromValueWithDescriptors(types.SerialMessage(rowbytes), kd, vd, t.ns)
	if err != nil {
		return nil, err
	}
	return IndexFromMapInterface(m), nil
}

func (t doltDevTable) SetTableRows(ctx context.Context, rows Index) (Table, error) {
	rowsbytes, err := rows.bytes()
	if err != nil {
		return nil, err
	}

	fields, err := t.fields()
	if err != nil {
		return nil, err
	}
	fields.rows = rowsbytes
	msg, err := fields.write()
	if err != nil {
		return nil, err
	}

	return doltDevTable{t.vrw, t.ns, msg}, nil
}

func (t doltDevTable) GetIndexes(ctx context.Context) (IndexSet, error) {
	ambytes := t.msg.SecondaryIndexesBytes()
	node, fileId, err := tree.NodeFromBytes(ambytes)
	if err != nil {
		return nil, err
	}
	if fileId != serial.AddressMapFileID {
		return nil, fmt.Errorf("unexpected file ID for secondary index map, expected %s, got %s", serial.AddressMapFileID, fileId)
	}
	ns := t.ns
	am, err := prolly.NewAddressMap(node, ns)
	if err != nil {
		return nil, err
	}
	return doltDevIndexSet{t.vrw, t.ns, am}, nil
}

func (t doltDevTable) SetIndexes(ctx context.Context, indexes IndexSet) (Table, error) {
	fields, err := t.fields()
	if err != nil {
		return nil, err
	}
	fields.indexes = indexes.(doltDevIndexSet).am
	msg, err := fields.write()
	if err != nil {
		return nil, err
	}
	return doltDevTable{t.vrw, t.ns, msg}, nil
}

func (t doltDevTable) GetConflicts(ctx context.Context) (conflict.ConflictSchema, ConflictIndex, error) {
	conflicts, err := t.msg.TryConflicts(nil)
	if err != nil {
		return conflict.ConflictSchema{}, nil, err
	}

	ouraddr := hash.New(conflicts.OurSchemaBytes())
	theiraddr := hash.New(conflicts.TheirSchemaBytes())
	baseaddr := hash.New(conflicts.AncestorSchemaBytes())

	if ouraddr.IsEmpty() {
		sch, err := t.GetSchema(ctx)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
		empty, err := NewEmptyConflictIndex(ctx, t.vrw, t.ns, sch, sch, sch)
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
		conflictIdx, err = NewEmptyConflictIndex(ctx, t.vrw, t.ns, ourschema, theirschema, baseschema)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
	} else {
		conflictIdx, err = conflictIndexFromAddr(ctx, t.vrw, t.ns, ourschema, theirschema, baseschema, mapaddr)
		if err != nil {
			return conflict.ConflictSchema{}, nil, err
		}
	}

	return conflictschema, conflictIdx, nil
}

// GetArtifacts implements Table.
func (t doltDevTable) GetArtifacts(ctx context.Context) (ArtifactIndex, error) {
	if t.Format() != types.Format_DOLT {
		panic("artifacts only implemented for DOLT")
	}

	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	addr := hash.New(t.msg.ArtifactsBytes())
	if addr.IsEmpty() {
		return NewEmptyArtifactIndex(ctx, t.vrw, t.ns, sch)
	}

	return artifactIndexFromAddr(ctx, t.vrw, t.ns, sch, addr)
}

// SetArtifacts implements Table.
func (t doltDevTable) SetArtifacts(ctx context.Context, artifacts ArtifactIndex) (Table, error) {
	if t.Format() != types.Format_DOLT {
		panic("artifacts only implemented for DOLT")
	}

	var addr hash.Hash
	if artifacts != nil {
		c, err := artifacts.Count()
		if err != nil {
			return nil, err
		}
		if c != 0 {
			ref, err := RefFromArtifactIndex(ctx, t.vrw, artifacts)
			if err != nil {
				return nil, err
			}
			addr = ref.TargetHash()
		}
	}
	msg := t.clone()
	copy(msg.ArtifactsBytes(), addr[:])
	return doltDevTable{t.vrw, t.ns, msg}, nil
}

func (t doltDevTable) HasConflicts(ctx context.Context) (bool, error) {

	conflicts, err := t.msg.TryConflicts(nil)
	if err != nil {
		return false, err
	}
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
	cmsg, err := msg.TryConflicts(nil)
	if err != nil {
		return nil, err
	}
	copy(cmsg.DataBytes(), conflictsAddr[:])
	copy(cmsg.OurSchemaBytes(), ouraddr[:])
	copy(cmsg.TheirSchemaBytes(), theiraddr[:])
	copy(cmsg.AncestorSchemaBytes(), baseaddr[:])

	return doltDevTable{t.vrw, t.ns, msg}, nil
}

func (t doltDevTable) ClearConflicts(ctx context.Context) (Table, error) {
	msg := t.clone()
	conflicts, err := msg.TryConflicts(nil)
	if err != nil {
		return nil, err
	}
	var emptyhash hash.Hash
	copy(conflicts.DataBytes(), emptyhash[:])
	copy(conflicts.OurSchemaBytes(), emptyhash[:])
	copy(conflicts.TheirSchemaBytes(), emptyhash[:])
	copy(conflicts.AncestorSchemaBytes(), emptyhash[:])
	return doltDevTable{t.vrw, t.ns, msg}, nil
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
	return doltDevTable{t.vrw, t.ns, msg}, nil
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
		fields, err := t.fields()
		if err != nil {
			return nil, err
		}
		fields.autoincval = val
		msg, err = fields.write()
		if err != nil {
			return nil, err
		}
	}
	return doltDevTable{t.vrw, t.ns, msg}, nil
}

func (t doltDevTable) clone() *serial.Table {
	bs := make([]byte, len(t.msg.Table().Bytes))
	copy(bs, t.msg.Table().Bytes)
	var ret serial.Table
	ret.Init(bs, t.msg.Table().Pos)
	return &ret
}

func (t doltDevTable) fields() (serialTableFields, error) {
	ambytes := t.msg.SecondaryIndexesBytes()
	node, fileId, err := tree.NodeFromBytes(ambytes)
	if err != nil {
		return serialTableFields{}, err
	}
	if fileId != serial.AddressMapFileID {
		return serialTableFields{}, fmt.Errorf("unexpected file ID for secondary index map, expected %s, got %s", serial.AddressMapFileID, fileId)
	}
	ns := t.ns

	conflicts, err := t.msg.TryConflicts(nil)
	if err != nil {
		return serialTableFields{}, err
	}
	am, err := prolly.NewAddressMap(node, ns)
	if err != nil {
		return serialTableFields{}, err
	}
	return serialTableFields{
		schema:            t.msg.SchemaBytes(),
		rows:              t.msg.PrimaryIndexBytes(),
		indexes:           am,
		conflictsdata:     conflicts.DataBytes(),
		conflictsours:     conflicts.OurSchemaBytes(),
		conflictstheirs:   conflicts.TheirSchemaBytes(),
		conflictsancestor: conflicts.AncestorSchemaBytes(),
		violations:        t.msg.ViolationsBytes(),
		artifacts:         t.msg.ArtifactsBytes(),
		autoincval:        t.msg.AutoIncrementValue(),
	}, nil
}

func getSchemaAtAddr(ctx context.Context, vrw types.ValueReadWriter, addr hash.Hash) (schema.Schema, error) {
	return encoding.UnmarshalSchemaAtAddr(ctx, vrw, addr)
}

func getAddrForSchema(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (hash.Hash, error) {
	st, err := encoding.MarshalSchema(ctx, vrw, sch)
	if err != nil {
		return hash.Hash{}, err
	}
	ref, err := vrw.WriteValue(ctx, st)
	if err != nil {
		return hash.Hash{}, err
	}
	return ref.TargetHash(), nil
}
