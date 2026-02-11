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

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
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

var (
	errNbfUnknown = fmt.Errorf("unknown NomsBinFormat")
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
	GetTableRowsWithDescriptors(ctx context.Context, kd, vd *val.TupleDesc) (Index, error)
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

	// GetAutoIncrement returns the AUTO_INCREMENT sequence value for this table.
	GetAutoIncrement(ctx context.Context) (uint64, error)
	// SetAutoIncrement sets the AUTO_INCREMENT sequence value for this table.
	SetAutoIncrement(ctx context.Context, val uint64) (Table, error)

	// DebugString returns the table contents for debugging purposes
	DebugString(ctx context.Context, ns tree.NodeStore) string
}

var sharePool = pool.NewBuffPool()

// NewTable returns a new Table.
func NewTable(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema, rows Index, indexes IndexSet, autoIncVal types.Value) (Table, error) {
	if vrw.Format().UsesFlatbuffers() {
		return newDoltDevTable(ctx, vrw, ns, sch, rows, indexes, autoIncVal)
	}

	panic("Unsupported format: " + vrw.Format().VersionString())
}

// TableFromAddr deserializes the table in the chunk at |addr|.
func TableFromAddr(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, addr hash.Hash) (Table, error) {
	val, err := vrw.MustReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	if !vrw.Format().UsesFlatbuffers() {
		panic("Unsupported format: " + vrw.Format().VersionString())
	}

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

// VrwFromTable returns the types.ValueReadWriter used by |t|.
// todo(andy): this is a temporary method that will be removed when there is a
// general-purpose abstraction to replace types.ValueReadWriter.
func VrwFromTable(t Table) types.ValueReadWriter {
	ddt := t.(doltDevTable)
	return ddt.vrw
}

func NodeStoreFromTable(t Table) tree.NodeStore {
	ddt := t.(doltDevTable)
	return ddt.ns
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

	schemaRef, err := vrw.WriteValue(ctx, schVal)
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

	schRef, err := t.vrw.WriteValue(ctx, newSchemaVal)
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

func (t doltDevTable) GetTableRowsWithDescriptors(ctx context.Context, kd, vd *val.TupleDesc) (Index, error) {
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

// GetAutoIncrement returns the next value to be used for the AUTO_INCREMENT column.
func (t doltDevTable) GetAutoIncrement(ctx context.Context) (uint64, error) {
	res := t.msg.AutoIncrementValue()
	if res == 0 {
		return 1, nil
	}
	return res, nil
}

// SetAutoIncrement sets the next value to be used for the AUTO_INCREMENT column.
// Since AUTO_INCREMENT starts at 1, setting this to either 0 or 1 will result in the field being unset.
func (t doltDevTable) SetAutoIncrement(ctx context.Context, val uint64) (Table, error) {
	// AUTO_INCREMENT starts at 1, so a value of 1 is the same as being unset.
	// Normalizing both values to 0 ensures that they both result in the same hash as the field being unset.
	if val == 1 {
		val = 0
	}
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

func RefFromNomsTable(ctx context.Context, table Table) (types.Ref, error) {
	ddt := table.(doltDevTable)
	return ddt.vrw.WriteValue(ctx, ddt.nomsValue())
}
