// Copyright 2022 Dolthub, Inc.
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
	"fmt"

	fb "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	fkBuilderSize = 2048
)

func DeserializeForeignKeys(ctx context.Context, nbf *types.NomsBinFormat, fks types.Value) (*ForeignKeyCollection, error) {
	if nbf.UsesFlatbuffers() {
		return deserializeFlatbufferForeignKeys(fks.(types.SerialMessage))
	} else {
		return deserializeNomsForeignKeys(ctx, fks.(types.Map))
	}
}

func SerializeForeignKeys(ctx context.Context, vrw types.ValueReadWriter, fkc *ForeignKeyCollection) (types.Value, error) {
	if vrw.Format().UsesFlatbuffers() {
		return serializeFlatbufferForeignKeys(fkc), nil
	} else {
		return serializeNomsForeignKeys(ctx, vrw, fkc)
	}
}

// deserializeNomsForeignKeys returns a new ForeignKeyCollection using the provided map returned previously by GetMap.
func deserializeNomsForeignKeys(ctx context.Context, fkMap types.Map) (*ForeignKeyCollection, error) {
	fkc := &ForeignKeyCollection{
		foreignKeys: make(map[string]ForeignKey),
	}
	err := fkMap.IterAll(ctx, func(key, value types.Value) error {
		foreignKey := &ForeignKey{}
		err := marshal.Unmarshal(ctx, fkMap.Format(), value, foreignKey)
		if err != nil {
			return err
		}
		fkc.foreignKeys[string(key.(types.String))] = *foreignKey
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fkc, nil
}

// serializeNomsForeignKeys serializes a ForeignKeyCollection as a types.Map.
func serializeNomsForeignKeys(ctx context.Context, vrw types.ValueReadWriter, fkc *ForeignKeyCollection) (types.Map, error) {
	fkMap, err := types.NewMap(ctx, vrw)
	if err != nil {
		return types.EmptyMap, err
	}
	fkMapEditor := fkMap.Edit()
	for hashOf, foreignKey := range fkc.foreignKeys {
		val, err := marshal.Marshal(ctx, vrw, foreignKey)
		if err != nil {
			return types.EmptyMap, err
		}
		fkMapEditor.Set(types.String(hashOf), val)
	}
	return fkMapEditor.Map(ctx)
}

// deserializeFlatbufferForeignKeys returns a new ForeignKeyCollection using the provided map returned previously by GetMap.
func deserializeFlatbufferForeignKeys(msg types.SerialMessage) (*ForeignKeyCollection, error) {
	if serial.GetFileID(msg) != serial.ForeignKeyCollectionFileID {
		return nil, fmt.Errorf("expect Serial Message with ForeignKeyCollectionFileID")
	}

	var c serial.ForeignKeyCollection
	err := serial.InitForeignKeyCollectionRoot(&c, msg, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}
	collection := &ForeignKeyCollection{
		foreignKeys: make(map[string]ForeignKey, c.ForeignKeysLength()),
	}

	var fk serial.ForeignKey
	for i := 0; i < c.ForeignKeysLength(); i++ {
		_, err = c.TryForeignKeys(&fk, i)
		if err != nil {
			return nil, err
		}

		childCols := make([]uint64, fk.ChildTableColumnsLength())
		for j := range childCols {
			childCols[j] = fk.ChildTableColumns(j)
		}
		parentCols := make([]uint64, fk.ParentTableColumnsLength())
		for j := range parentCols {
			parentCols[j] = fk.ParentTableColumns(j)
		}

		var childUnresolved []string
		cn := fk.UnresolvedChildColumnsLength()
		if cn > 0 {
			childUnresolved = make([]string, cn)
			for j := range childUnresolved {
				childUnresolved[j] = string(fk.UnresolvedChildColumns(j))
			}
		}
		var parentUnresolved []string
		pn := fk.UnresolvedParentColumnsLength()
		if pn > 0 {
			parentUnresolved = make([]string, pn)
			for j := range parentUnresolved {
				parentUnresolved[j] = string(fk.UnresolvedParentColumns(j))
			}
		}

		tableName, ok := decodeTableNameFromSerialization(string(fk.ChildTableName()))
		if !ok {
			return nil, fmt.Errorf("could not decode table name: %s", string(fk.ChildTableName()))
		}

		parentTableName, ok := decodeTableNameFromSerialization(string(fk.ParentTableName()))
		if !ok {
			return nil, fmt.Errorf("could not decode table name: %s", string(fk.ParentTableName()))
		}

		err := collection.AddKeys(ForeignKey{
			Name:                   string(fk.Name()),
			TableName:              tableName,
			TableIndex:             string(fk.ChildTableIndex()),
			TableColumns:           childCols,
			ReferencedTableName:    parentTableName,
			ReferencedTableIndex:   string(fk.ParentTableIndex()),
			ReferencedTableColumns: parentCols,
			OnUpdate:               ForeignKeyReferentialAction(fk.OnUpdate()),
			OnDelete:               ForeignKeyReferentialAction(fk.OnDelete()),
			UnresolvedFKDetails: UnresolvedFKDetails{
				TableColumns:           childUnresolved,
				ReferencedTableColumns: parentUnresolved,
			},
		})
		if err != nil {
			return nil, err
		}
	}
	return collection, nil
}

// serializeFlatbufferForeignKeys serializes a ForeignKeyCollection as a types.Map.
func serializeFlatbufferForeignKeys(fkc *ForeignKeyCollection) types.SerialMessage {
	foreignKeys := fkc.AllKeys()
	offsets := make([]fb.UOffsetT, len(foreignKeys))
	b := fb.NewBuilder(fkBuilderSize)

	for i := len(foreignKeys) - 1; i >= 0; i-- {
		var (
			foreignKeyName   fb.UOffsetT
			childTable       fb.UOffsetT
			childIndex       fb.UOffsetT
			childCols        fb.UOffsetT
			parentTable      fb.UOffsetT
			parentIndex      fb.UOffsetT
			parentCols       fb.UOffsetT
			unresolvedChild  fb.UOffsetT
			unresolvedParent fb.UOffsetT
		)

		fk := foreignKeys[i]
		if fk.UnresolvedFKDetails.ReferencedTableColumns != nil {
			unresolvedParent = datas.SerializeStringVector(b, fk.UnresolvedFKDetails.ReferencedTableColumns)
		}
		if fk.UnresolvedFKDetails.TableColumns != nil {
			unresolvedChild = datas.SerializeStringVector(b, fk.UnresolvedFKDetails.TableColumns)
		}
		parentCols = serializeUint64Vector(b, fk.ReferencedTableColumns)
		childCols = serializeUint64Vector(b, fk.TableColumns)
		parentTable = b.CreateString(encodeTableNameForSerialization(fk.ReferencedTableName))
		parentIndex = b.CreateString(fk.ReferencedTableIndex)
		childTable = b.CreateString(encodeTableNameForSerialization(fk.TableName))
		childIndex = b.CreateString(fk.TableIndex)
		foreignKeyName = b.CreateString(fk.Name)

		serial.ForeignKeyStart(b)
		serial.ForeignKeyAddName(b, foreignKeyName)
		serial.ForeignKeyAddChildTableName(b, childTable)
		serial.ForeignKeyAddChildTableIndex(b, childIndex)
		serial.ForeignKeyAddChildTableColumns(b, childCols)
		serial.ForeignKeyAddParentTableName(b, parentTable)
		serial.ForeignKeyAddParentTableIndex(b, parentIndex)
		serial.ForeignKeyAddParentTableColumns(b, parentCols)
		serial.ForeignKeyAddUnresolvedChildColumns(b, unresolvedChild)
		serial.ForeignKeyAddUnresolvedParentColumns(b, unresolvedParent)
		serial.ForeignKeyAddOnUpdate(b, serial.ForeignKeyReferentialAction(fk.OnUpdate))
		serial.ForeignKeyAddOnDelete(b, serial.ForeignKeyReferentialAction(fk.OnDelete))
		offsets[i] = serial.ForeignKeyEnd(b)
	}

	serial.ForeignKeyCollectionStartForeignKeysVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	vec := b.EndVector(len(offsets))

	serial.ForeignKeyCollectionStart(b)
	serial.ForeignKeyCollectionAddForeignKeys(b, vec)
	o := serial.ForeignKeyCollectionEnd(b)
	return []byte(serial.FinishMessage(b, o, []byte(serial.ForeignKeyCollectionFileID)))
}

func serializeUint64Vector(b *fb.Builder, u []uint64) fb.UOffsetT {
	b.StartVector(8, len(u), 8)
	for j := len(u) - 1; j >= 0; j-- {
		b.PrependUint64(u[j])
	}
	return b.EndVector(len(u))
}

func EmptyForeignKeyCollection(msg types.SerialMessage) (bool, error) {
	if serial.GetFileID(msg) != serial.ForeignKeyCollectionFileID {
		return false, nil
	}
	var c serial.ForeignKeyCollection
	err := serial.InitForeignKeyCollectionRoot(&c, msg, serial.MessagePrefixSz)
	if err != nil {
		return false, err
	}
	return c.ForeignKeysLength() == 0, nil
}
