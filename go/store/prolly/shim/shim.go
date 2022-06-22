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

package shim

import (
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func NodeFromValue(v types.Value) tree.Node {
	return tree.NodeFromBytes(v.(types.TupleRowStorage))
}

func ValueFromMap(m prolly.Map) types.Value {
	return tree.ValueFromNode(m.Node())
}

func ValueFromConflictMap(m prolly.ConflictMap) types.Value {
	return tree.ValueFromNode(m.Node())
}

func ValueFromArtifactMap(m prolly.ArtifactMap) types.Value {
	return tree.ValueFromNode(m.Node())
}

func MapFromValue(v types.Value, sch schema.Schema, vrw types.ValueReadWriter) prolly.Map {
	root := NodeFromValue(v)
	kd := KeyDescriptorFromSchema(sch)
	vd := ValueDescriptorFromSchema(sch)
	ns := tree.NewNodeStore(ChunkStoreFromVRW(vrw))
	return prolly.NewMap(root, ns, kd, vd)
}

func ConflictMapFromValue(v types.Value, ourSchema, theirSchema, baseSchema schema.Schema, vrw types.ValueReadWriter) prolly.ConflictMap {
	root := NodeFromValue(v)
	kd, ourVD := MapDescriptorsFromSchema(ourSchema)
	theirVD := ValueDescriptorFromSchema(theirSchema)
	baseVD := ValueDescriptorFromSchema(baseSchema)
	ns := tree.NewNodeStore(ChunkStoreFromVRW(vrw))
	return prolly.NewConflictMap(root, ns, kd, ourVD, theirVD, baseVD)
}

func ChunkStoreFromVRW(vrw types.ValueReadWriter) chunks.ChunkStore {
	switch x := vrw.(type) {
	case datas.Database:
		return datas.ChunkStoreFromDatabase(x)
	case *types.ValueStore:
		return x.ChunkStore()
	}
	panic("unknown ValueReadWriter")
}

func MapDescriptorsFromSchema(sch schema.Schema) (kd, vd val.TupleDesc) {
	kd = KeyDescriptorFromSchema(sch)
	vd = ValueDescriptorFromSchema(sch)
	return
}

func KeyDescriptorFromSchema(sch schema.Schema) val.TupleDesc {
	if schema.IsKeyless(sch) {
		return val.KeylessTupleDesc
	}

	var tt []val.Type
	_ = sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		tt = append(tt, val.Type{
			Enc:      encodingFromSqlType(col.TypeInfo.ToSqlType().Type()),
			Nullable: columnNullable(col),
		})
		return
	})
	return val.NewTupleDescriptor(tt...)
}

func columnNullable(col schema.Column) bool {
	for _, cnst := range col.Constraints {
		if cnst.GetConstraintType() == schema.NotNullConstraintType {
			return false
		}
	}
	return true
}

func ValueDescriptorFromSchema(sch schema.Schema) val.TupleDesc {
	var tt []val.Type
	if schema.IsKeyless(sch) {
		tt = []val.Type{val.KeylessCardType}
	}

	_ = sch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		tt = append(tt, val.Type{
			Enc:      encodingFromSqlType(col.TypeInfo.ToSqlType().Type()),
			Nullable: col.IsNullable(),
		})
		return
	})
	return val.NewTupleDescriptor(tt...)
}

func encodingFromSqlType(typ query.Type) val.Encoding {
	return val.Encoding(schema.EncodingFromSqlType(typ))
}
