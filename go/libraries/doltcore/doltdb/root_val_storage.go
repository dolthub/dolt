// Copyright 2024 Dolthub, Inc.
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
	"bytes"
	"context"
	"fmt"
	"strings"

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type rootValueStorage interface {
	GetFeatureVersion() (FeatureVersion, bool, error)

	GetTablesMap(ctx context.Context, vr types.ValueReadWriter, ns tree.NodeStore, databaseSchema string) (tableMap, error)
	GetForeignKeys(ctx context.Context, vr types.ValueReader) (types.Value, bool, error)
	GetCollation(ctx context.Context) (schema.Collation, error)
	GetSchemas(ctx context.Context) ([]schema.DatabaseSchema, error)

	SetForeignKeyMap(ctx context.Context, vrw types.ValueReadWriter, m types.Value) (rootValueStorage, error)
	SetFeatureVersion(v FeatureVersion) (rootValueStorage, error)
	SetCollation(ctx context.Context, collation schema.Collation) (rootValueStorage, error)
	SetSchemas(ctx context.Context, dbSchemas []schema.DatabaseSchema) (rootValueStorage, error)

	EditTablesMap(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, edits []tableEdit) (rootValueStorage, error)

	DebugString(ctx context.Context) string
	nomsValue() types.Value
}

type nomsRvStorage struct {
	valueSt types.Struct
}

type tableMap interface {
	Get(ctx context.Context, name string) (hash.Hash, error)
	Iter(ctx context.Context, cb func(name string, addr hash.Hash) (bool, error)) error
}

func tmIterAll(ctx context.Context, tm tableMap, cb func(name string, addr hash.Hash)) error {
	return tm.Iter(ctx, func(name string, addr hash.Hash) (bool, error) {
		cb(name, addr)
		return false, nil
	})
}

func (r nomsRvStorage) GetFeatureVersion() (FeatureVersion, bool, error) {
	v, ok, err := r.valueSt.MaybeGet(featureVersKey)
	if err != nil {
		return 0, false, err
	}
	if ok {
		return FeatureVersion(v.(types.Int)), true, nil
	} else {
		return 0, false, nil
	}
}

func (r nomsRvStorage) GetTablesMap(context.Context, types.ValueReadWriter, tree.NodeStore, string) (tableMap, error) {
	v, found, err := r.valueSt.MaybeGet(tablesKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nomsTableMap{types.EmptyMap}, nil
	}
	return nomsTableMap{v.(types.Map)}, nil
}

func (ntm nomsTableMap) Get(ctx context.Context, name string) (hash.Hash, error) {
	v, f, err := ntm.MaybeGet(ctx, types.String(name))
	if err != nil {
		return hash.Hash{}, err
	}
	if !f {
		return hash.Hash{}, nil
	}
	return v.(types.Ref).TargetHash(), nil
}

func (ntm nomsTableMap) Iter(ctx context.Context, cb func(name string, addr hash.Hash) (bool, error)) error {
	return ntm.Map.Iter(ctx, func(k, v types.Value) (bool, error) {
		name := string(k.(types.String))
		addr := v.(types.Ref).TargetHash()
		return cb(name, addr)
	})
}

func (r nomsRvStorage) GetForeignKeys(context.Context, types.ValueReader) (types.Value, bool, error) {
	v, found, err := r.valueSt.MaybeGet(foreignKeyKey)
	if err != nil {
		return types.Map{}, false, err
	}
	if !found {
		return types.Map{}, false, err
	}
	return v.(types.Map), true, nil
}

func (r nomsRvStorage) GetCollation(ctx context.Context) (schema.Collation, error) {
	v, found, err := r.valueSt.MaybeGet(rootCollationKey)
	if err != nil {
		return schema.Collation_Unspecified, err
	}
	if !found {
		return schema.Collation_Default, nil
	}
	return schema.Collation(v.(types.Uint)), nil
}

func (r nomsRvStorage) EditTablesMap(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, edits []tableEdit) (rootValueStorage, error) {
	m, err := r.GetTablesMap(ctx, vrw, ns, "")
	if err != nil {
		return nil, err
	}
	nm := m.(nomsTableMap).Map

	me := nm.Edit()
	for _, e := range edits {
		if e.old_name.Name != "" {
			old, f, err := nm.MaybeGet(ctx, types.String(e.old_name.Name))
			if err != nil {
				return nil, err
			}
			if !f {
				return nil, ErrTableNotFound
			}
			_, f, err = nm.MaybeGet(ctx, types.String(e.name.Name))
			if err != nil {
				return nil, err
			}
			if f {
				return nil, ErrTableExists
			}
			me = me.Remove(types.String(e.old_name.Name)).Set(types.String(e.name.Name), old)
		} else {
			if e.ref == nil {
				me = me.Remove(types.String(e.name.Name))
			} else {
				me = me.Set(types.String(e.name.Name), *e.ref)
			}
		}
	}

	nm, err = me.Map(ctx)
	if err != nil {
		return nil, err
	}

	st, err := r.valueSt.Set(tablesKey, nm)
	if err != nil {
		return nil, err
	}
	return nomsRvStorage{st}, nil
}

func (r nomsRvStorage) SetForeignKeyMap(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (rootValueStorage, error) {
	st, err := r.valueSt.Set(foreignKeyKey, v)
	if err != nil {
		return nomsRvStorage{}, err
	}
	return nomsRvStorage{st}, nil
}

func (r nomsRvStorage) SetFeatureVersion(v FeatureVersion) (rootValueStorage, error) {
	st, err := r.valueSt.Set(featureVersKey, types.Int(v))
	if err != nil {
		return nomsRvStorage{}, err
	}
	return nomsRvStorage{st}, nil
}

func (r nomsRvStorage) SetCollation(ctx context.Context, collation schema.Collation) (rootValueStorage, error) {
	st, err := r.valueSt.Set(rootCollationKey, types.Uint(collation))
	if err != nil {
		return nomsRvStorage{}, err
	}
	return nomsRvStorage{st}, nil
}

func (r nomsRvStorage) GetSchemas(ctx context.Context) ([]schema.DatabaseSchema, error) {
	// stub implementation, used only for migration
	return nil, nil
}

func (r nomsRvStorage) SetSchemas(ctx context.Context, dbSchemas []schema.DatabaseSchema) (rootValueStorage, error) {
	panic("schemas not implemented for nomsRvStorage")
}

func (r nomsRvStorage) DebugString(ctx context.Context) string {
	var buf bytes.Buffer
	err := types.WriteEncodedValue(ctx, &buf, r.valueSt)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func (r nomsRvStorage) nomsValue() types.Value {
	return r.valueSt
}

type nomsTableMap struct {
	types.Map
}

type fbRvStorage struct {
	srv *serial.RootValue
}

func (r fbRvStorage) SetForeignKeyMap(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (rootValueStorage, error) {
	var h hash.Hash
	isempty, err := EmptyForeignKeyCollection(v.(types.SerialMessage))
	if err != nil {
		return nil, err
	}
	if !isempty {
		ref, err := vrw.WriteValue(ctx, v)
		if err != nil {
			return nil, err
		}
		h = ref.TargetHash()
	}
	ret := r.clone()
	copy(ret.srv.ForeignKeyAddrBytes(), h[:])
	return ret, nil
}

func (r fbRvStorage) SetFeatureVersion(v FeatureVersion) (rootValueStorage, error) {
	ret := r.clone()
	ret.srv.MutateFeatureVersion(int64(v))
	return ret, nil
}

func (r fbRvStorage) SetCollation(ctx context.Context, collation schema.Collation) (rootValueStorage, error) {
	ret := r.clone()
	ret.srv.MutateCollation(serial.Collation(collation))
	return ret, nil
}

func (r fbRvStorage) GetSchemas(ctx context.Context) ([]schema.DatabaseSchema, error) {
	numSchemas := r.srv.SchemasLength()
	schemas := make([]schema.DatabaseSchema, numSchemas)
	for i := 0; i < numSchemas; i++ {
		dbSchema := new(serial.DatabaseSchema)
		_, err := r.srv.TrySchemas(dbSchema, i)
		if err != nil {
			return nil, err
		}

		schemas[i] = schema.DatabaseSchema{
			Name: string(dbSchema.Name()),
		}
	}

	return schemas, nil
}

func (r fbRvStorage) SetSchemas(ctx context.Context, dbSchemas []schema.DatabaseSchema) (rootValueStorage, error) {
	msg, err := r.serializeRootValue(r.srv.TablesBytes(), dbSchemas)
	if err != nil {
		return nil, err
	}
	return fbRvStorage{msg}, nil
}

func (r fbRvStorage) clone() fbRvStorage {
	bs := make([]byte, len(r.srv.Table().Bytes))
	copy(bs, r.srv.Table().Bytes)
	var ret serial.RootValue
	ret.Init(bs, r.srv.Table().Pos)
	return fbRvStorage{&ret}
}

func (r fbRvStorage) DebugString(ctx context.Context) string {
	return fmt.Sprintf("fbRvStorage[%d, %s, %s]",
		r.srv.FeatureVersion(),
		"...", // TODO: Print out tables map
		hash.New(r.srv.ForeignKeyAddrBytes()).String())
}

func (r fbRvStorage) nomsValue() types.Value {
	return types.SerialMessage(r.srv.Table().Bytes)
}

func (r fbRvStorage) GetFeatureVersion() (FeatureVersion, bool, error) {
	return FeatureVersion(r.srv.FeatureVersion()), true, nil
}

func (r fbRvStorage) getAddressMap(vrw types.ValueReadWriter, ns tree.NodeStore) (prolly.AddressMap, error) {
	tbytes := r.srv.TablesBytes()
	node, err := shim.NodeFromValue(types.SerialMessage(tbytes))
	if err != nil {
		return prolly.AddressMap{}, err
	}
	return prolly.NewAddressMap(node, ns)
}

func (r fbRvStorage) GetTablesMap(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, databaseSchema string) (tableMap, error) {
	am, err := r.getAddressMap(vrw, ns)
	if err != nil {
		return nil, err
	}
	return fbTableMap{AddressMap: am, schemaName: databaseSchema}, nil
}

func (r fbRvStorage) GetForeignKeys(ctx context.Context, vr types.ValueReader) (types.Value, bool, error) {
	addr := hash.New(r.srv.ForeignKeyAddrBytes())
	if addr.IsEmpty() {
		return types.SerialMessage{}, false, nil
	}
	v, err := vr.ReadValue(ctx, addr)
	if err != nil {
		return types.SerialMessage{}, false, err
	}
	return v.(types.SerialMessage), true, nil
}

func (r fbRvStorage) GetCollation(ctx context.Context) (schema.Collation, error) {
	collation := r.srv.Collation()
	// Pre-existing repositories will return invalid here
	if collation == serial.Collationinvalid {
		return schema.Collation_Default, nil
	}
	return schema.Collation(collation), nil
}

func (r fbRvStorage) EditTablesMap(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, edits []tableEdit) (rootValueStorage, error) {
	am, err := r.getAddressMap(vrw, ns)
	if err != nil {
		return nil, err
	}
	ae := am.Editor()
	for _, e := range edits {
		if e.old_name.Name != "" {
			oldaddr, err := am.Get(ctx, e.old_name.Name)
			if err != nil {
				return nil, err
			}
			newaddr, err := am.Get(ctx, encodeTableNameForSerialization(e.name))
			if err != nil {
				return nil, err
			}
			if oldaddr.IsEmpty() {
				return nil, ErrTableNotFound
			}
			if !newaddr.IsEmpty() {
				return nil, ErrTableExists
			}
			err = ae.Delete(ctx, e.old_name.Name)
			if err != nil {
				return nil, err
			}
			err = ae.Update(ctx, encodeTableNameForSerialization(e.name), oldaddr)
			if err != nil {
				return nil, err
			}
		} else {
			if e.ref == nil {
				err := ae.Delete(ctx, encodeTableNameForSerialization(e.name))
				if err != nil {
					return nil, err
				}
			} else {
				err := ae.Update(ctx, encodeTableNameForSerialization(e.name), e.ref.TargetHash())
				if err != nil {
					return nil, err
				}
			}
		}
	}
	am, err = ae.Flush(ctx)
	if err != nil {
		return nil, err
	}

	ambytes := []byte(tree.ValueFromNode(am.Node()).(types.SerialMessage))
	dbSchemas, err := r.GetSchemas(ctx)
	if err != nil {
		return nil, err
	}

	msg, err := r.serializeRootValue(ambytes, dbSchemas)
	if err != nil {
		return nil, err
	}
	return fbRvStorage{msg}, nil
}

func (r fbRvStorage) serializeRootValue(addressMapBytes []byte, dbSchemas []schema.DatabaseSchema) (*serial.RootValue, error) {
	builder := flatbuffers.NewBuilder(80)
	tablesoff := builder.CreateByteVector(addressMapBytes)
	schemasOff := serializeDatabaseSchemas(builder, dbSchemas)

	fkoff := builder.CreateByteVector(r.srv.ForeignKeyAddrBytes())
	serial.RootValueStart(builder)
	serial.RootValueAddFeatureVersion(builder, r.srv.FeatureVersion())
	serial.RootValueAddCollation(builder, r.srv.Collation())
	serial.RootValueAddTables(builder, tablesoff)
	serial.RootValueAddForeignKeyAddr(builder, fkoff)
	if schemasOff > 0 {
		serial.RootValueAddSchemas(builder, schemasOff)
	}

	bs := serial.FinishMessage(builder, serial.RootValueEnd(builder), []byte(serial.RootValueFileID))
	msg, err := serial.TryGetRootAsRootValue(bs, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func serializeDatabaseSchemas(b *flatbuffers.Builder, dbSchemas []schema.DatabaseSchema) flatbuffers.UOffsetT {
	// if we have no schemas, do not serialize an empty vector
	if len(dbSchemas) == 0 {
		return 0
	}

	offsets := make([]flatbuffers.UOffsetT, len(dbSchemas))
	for i := len(dbSchemas) - 1; i >= 0; i-- {
		dbSchema := dbSchemas[i]

		nameOff := b.CreateString(dbSchema.Name)
		serial.DatabaseSchemaStart(b)
		serial.DatabaseSchemaAddName(b, nameOff)
		offsets[i] = serial.DatabaseSchemaEnd(b)
	}

	serial.RootValueStartSchemasVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}

// encodeTableNameForSerialization encodes a table name for serialization. Table names with no schema are encoded as
// just the bare table name. Table names with schemas are encoded by surrounding the schema name with null bytes and
// appending the table name.
func encodeTableNameForSerialization(name TableName) string {
	if name.Schema == "" {
		return name.Name
	}
	return fmt.Sprintf("\000%s\000%s", name.Schema, name.Name)
}

// decodeTableNameFromSerialization decodes a table name from a serialized string. See notes on serialization in
// |encodeTableNameForSerialization|
func decodeTableNameFromSerialization(encodedName string) (TableName, bool) {
	if encodedName[0] != 0 {
		return TableName{Name: encodedName}, true
	} else if len(encodedName) >= 4 { // 2 null bytes plus at least one char for schema and table name
		schemaEnd := strings.LastIndexByte(encodedName, 0)
		return TableName{
			Schema: encodedName[1:schemaEnd],
			Name:   encodedName[schemaEnd+1:],
		}, true
	}

	// invalid encoding
	return TableName{}, false
}

// decodeTableNameForAddressMap decodes a table name from an address map key, expecting a particular schema name. See
// notes on serialization in |encodeTableNameForSerialization|
func decodeTableNameForAddressMap(encodedName, schemaName string) (string, bool) {
	if schemaName == "" && encodedName[0] != 0 {
		return encodedName, true
	} else if schemaName != "" && encodedName[0] == 0 &&
		len(encodedName) > len(schemaName)+2 &&
		encodedName[1:len(schemaName)+1] == schemaName {
		return encodedName[len(schemaName)+2:], true
	}
	return "", false
}

type fbTableMap struct {
	prolly.AddressMap
	schemaName string
}

func (m fbTableMap) Get(ctx context.Context, name string) (hash.Hash, error) {
	return m.AddressMap.Get(ctx, encodeTableNameForSerialization(TableName{Name: name, Schema: m.schemaName}))
}

func (m fbTableMap) Iter(ctx context.Context, cb func(string, hash.Hash) (bool, error)) error {
	var stop bool
	return m.AddressMap.IterAll(ctx, func(n string, a hash.Hash) error {
		n, ok := decodeTableNameForAddressMap(n, m.schemaName)
		if !stop && ok {
			var err error
			stop, err = cb(n, a)
			return err
		}
		return nil
	})
}
