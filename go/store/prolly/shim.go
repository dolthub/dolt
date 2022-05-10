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

package prolly

import (
	"fmt"

	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func NodeFromValue(v types.Value) tree.Node {
	return tree.NodeFromBytes(v.(types.TupleRowStorage))
}

func ValueFromMap(m Map) types.Value {
	return tree.ValueFromNode(m.tuples.root)
}

func ValueFromConflictMap(m ConflictMap) types.Value {
	return tree.ValueFromNode(m.conflicts.root)
}

func MapFromValue(v types.Value, sch schema.Schema, vrw types.ValueReadWriter) Map {
	root := NodeFromValue(v)
	kd := KeyDescriptorFromSchema(sch)
	vd := ValueDescriptorFromSchema(sch)
	ns := tree.NewNodeStore(ChunkStoreFromVRW(vrw))
	return NewMap(root, ns, kd, vd)
}

func ConflictMapFromValue(v types.Value, ourSchema, theirSchema, baseSchema schema.Schema, vrw types.ValueReadWriter) ConflictMap {
	root := NodeFromValue(v)
	kd, ourVD := MapDescriptorsFromScheam(ourSchema)
	theirVD := ValueDescriptorFromSchema(theirSchema)
	baseVD := ValueDescriptorFromSchema(baseSchema)
	ns := tree.NewNodeStore(ChunkStoreFromVRW(vrw))
	return NewConflictMap(root, ns, kd, ourVD, theirVD, baseVD)
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

func MapDescriptorsFromScheam(sch schema.Schema) (kd, vd val.TupleDesc) {
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

// todo(andy): move this to typeinfo
func encodingFromSqlType(typ query.Type) val.Encoding {
	// todo(andy): replace temp encodings
	switch typ {
	case query.Type_DECIMAL:
		return val.DecimalEnc
	case query.Type_DATE:
		return val.DateEnc
	case query.Type_DATETIME:
		return val.DatetimeEnc
	case query.Type_TIME:
		return val.TimeEnc
	case query.Type_TIMESTAMP:
		return val.TimestampEnc
	case query.Type_YEAR:
		return val.YearEnc
	case query.Type_GEOMETRY:
		return val.GeometryEnc
	}

	switch typ {
	case query.Type_INT8:
		return val.Int8Enc
	case query.Type_UINT8:
		return val.Uint8Enc
	case query.Type_INT16:
		return val.Int16Enc
	case query.Type_UINT16:
		return val.Uint16Enc
	case query.Type_INT24:
		return val.Int32Enc
	case query.Type_UINT24:
		return val.Uint32Enc
	case query.Type_INT32:
		return val.Int32Enc
	case query.Type_UINT32:
		return val.Uint32Enc
	case query.Type_INT64:
		return val.Int64Enc
	case query.Type_UINT64:
		return val.Uint64Enc
	case query.Type_FLOAT32:
		return val.Float32Enc
	case query.Type_FLOAT64:
		return val.Float64Enc
	case query.Type_BIT:
		return val.Uint64Enc
	case query.Type_BINARY:
		return val.ByteStringEnc
	case query.Type_VARBINARY:
		return val.ByteStringEnc
	case query.Type_BLOB:
		return val.ByteStringEnc
	case query.Type_CHAR:
		return val.StringEnc
	case query.Type_VARCHAR:
		return val.StringEnc
	case query.Type_TEXT:
		return val.StringEnc
	case query.Type_JSON:
		return val.JSONEnc
	case query.Type_ENUM:
		return val.StringEnc
	case query.Type_SET:
		return val.StringEnc
	default:
		panic(fmt.Sprintf("unknown encoding %v", typ))
	}
}
