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

package typeinfo

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// extendedType is a type that refers to an ExtendedType in GMS. These are only supported in the new format, and have many
// more limitations than traditional types (for now).
type extendedType struct {
	sqlExtendedType sql.ExtendedType
	enc             val.Encoding // 0 means use default based on MaxSerializedWidth
}

var _ TypeInfo = (*extendedType)(nil)

// CreateExtendedTypeFromSqlType creates a TypeInfo from the given extended type with the default encoding.
func CreateExtendedTypeFromSqlType(typ sql.ExtendedType) TypeInfo {
	return &extendedType{sqlExtendedType: typ}
}

// Equals implements the TypeInfo interface.
func (ti *extendedType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*extendedType); ok {
		return ti.sqlExtendedType.Equals(ti2.sqlExtendedType)
	}
	return false
}

// NomsKind implements the TypeInfo interface.
func (ti *extendedType) NomsKind() types.NomsKind {
	return types.ExtendedKind
}

// String implements the TypeInfo interface.
func (ti *extendedType) String() string {
	return ti.sqlExtendedType.String()
}

// Encoding implements the TypeInfo interface.
func (ti *extendedType) Encoding() val.Encoding {
	if ti.enc != 0 {
		return ti.enc
	}
	switch ti.sqlExtendedType.MaxSerializedWidth() {
	case sql.ExtendedTypeSerializedWidth_64K:
		return val.ExtendedEnc
	case sql.ExtendedTypeSerializedWidth_Unbounded:
		return val.ExtendedAdaptiveEnc
	default:
		panic(fmt.Errorf("unknown extended type serialization width"))
	}
}

// WithEncoding implements the TypeInfo interface.
func (ti *extendedType) WithEncoding(enc val.Encoding) TypeInfo {
	return &extendedType{sqlExtendedType: ti.sqlExtendedType, enc: enc}
}

// ToSqlType implements the TypeInfo interface.
func (ti *extendedType) ToSqlType() sql.Type {
	return ti.sqlExtendedType
}
