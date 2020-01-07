// Copyright 2019 Liquidata, Inc.
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

	"github.com/src-d/go-mysql-server/sql"
	"vitess.io/vitess/go/sqltypes"

	"github.com/liquidata-inc/dolt/go/store/types"
)

type Identifier string

const (
	UnknownType Identifier = "unknown"
	NullType    Identifier = "null"
	Int8Type    Identifier = "int8"
	Int16Type   Identifier = "int16"
	Int24Type   Identifier = "int24"
	Int32Type   Identifier = "int32"
	Int64Type   Identifier = "int64"
	Uint8Type   Identifier = "uint8"
	Uint16Type  Identifier = "uint16"
	Uint24Type  Identifier = "uint24"
	Uint32Type  Identifier = "uint32"
	Uint64Type  Identifier = "uint64"
	Float32Type Identifier = "float32"
	Float64Type Identifier = "float64"
)

var Identifiers = map[Identifier]struct{}{
	UnknownType: struct{}{},
	NullType:    struct{}{},
	Int8Type:    struct{}{},
	Int16Type:   struct{}{},
	Int24Type:   struct{}{},
	Int32Type:   struct{}{},
	Int64Type:   struct{}{},
	Uint8Type:   struct{}{},
	Uint16Type:  struct{}{},
	Uint24Type:  struct{}{},
	Uint32Type:  struct{}{},
	Uint64Type:  struct{}{},
	Float32Type: struct{}{},
	Float64Type: struct{}{},
}

// TypeInfo is an interface used for encoding type information.
type TypeInfo interface {
	// AppliesToKind takes in a NomsKind and returns an error regarding whether this type applies to the value.
	AppliesToKind(kind types.NomsKind) error

	// ConvertNomsValueToValue converts a Noms value to a go value.
	ConvertNomsValueToValue(v types.Value) (interface{}, error)

	// ConvertValueToNomsValue converts a go value to a Noms value. The type of the Noms value will be
	// equivalent to the NomsKind returned from PreferredNomsKind.
	ConvertValueToNomsValue(v interface{}) (types.Value, error)

	// Equals returns whether the given TypeInfo is equivalent to this TypeInfo.
	Equals(other TypeInfo) bool

	// GetTypeIdentifier returns an identifier for this type used for serialization.
	GetTypeIdentifier() Identifier

	// GetTypeParams returns a map[string]string containing the type parameters.  This is used for
	// serialization and deserialization of type information.
	GetTypeParams() map[string]string

	// IsValid takes in a value (not a types.Value) and returns whether the value is valid for this
	// type.
	IsValid(v interface{}) bool

	// ToSqlType returns the TypeInfo as a sql.Type.
	ToSqlType() (sql.Type, error)

	// PreferredNomsKind returns the NomsKind that best matches this TypeInfo.
	PreferredNomsKind() types.NomsKind

	// Stringer results are used to inform users of the constraint's properties.
	fmt.Stringer
}

// TypeInfoFromSqlType takes in a sql.Type and returns the relevant TypeInfo.
func TypeInfoFromSqlType(sqlType sql.Type) (TypeInfo, error) {
	switch sqlType.Type() {
	case sqltypes.Null:
		return &nullImpl{}, nil
	case sqltypes.Int8:
		return &int8Impl{}, nil
	case sqltypes.Uint8:
		return &uint8Impl{}, nil
	case sqltypes.Int16:
		return &int16Impl{}, nil
	case sqltypes.Uint16:
		return &uint16Impl{}, nil
	case sqltypes.Int24:
		return &int24Impl{}, nil
	case sqltypes.Uint24:
		return &uint24Impl{}, nil
	case sqltypes.Int32:
		return &int32Impl{}, nil
	case sqltypes.Uint32:
		return &uint32Impl{}, nil
	case sqltypes.Int64:
		return &int64Impl{}, nil
	case sqltypes.Uint64:
		return &uint64Impl{}, nil
	case sqltypes.Float32:
		return &float32Impl{}, nil
	case sqltypes.Float64:
		return &float64Impl{}, nil
	case sqltypes.Timestamp:
		return &unknownImpl{}, nil
	case sqltypes.Date:
		return &unknownImpl{}, nil
	case sqltypes.Time:
		return &unknownImpl{}, nil
	case sqltypes.Datetime:
		return &unknownImpl{}, nil
	case sqltypes.Year:
		return &unknownImpl{}, nil
	case sqltypes.Decimal:
		return &unknownImpl{}, nil
	case sqltypes.Text:
		return &unknownImpl{}, nil
	case sqltypes.Blob:
		return &unknownImpl{}, nil
	case sqltypes.VarChar:
		return &unknownImpl{}, nil
	case sqltypes.VarBinary:
		return &unknownImpl{}, nil
	case sqltypes.Char:
		return &unknownImpl{}, nil
	case sqltypes.Binary:
		return &unknownImpl{}, nil
	case sqltypes.Bit:
		return &unknownImpl{}, nil
	case sqltypes.Enum:
		return &unknownImpl{}, nil
	case sqltypes.Set:
		return &unknownImpl{}, nil
	case sqltypes.TypeJSON:
		return &unknownImpl{}, nil
	case sqltypes.Expression:
		return &unknownImpl{}, nil
	default:
		return nil, fmt.Errorf(`no type info can be created from SQL base type "%v"`, sqlType.String())
	}
}

// TypeInfoFromIdentifierParams constructs a TypeInfo from the given identifier and parameters.
func TypeInfoFromIdentifierParams(id Identifier, params map[string]string) (TypeInfo, error) {
	switch id {
	case UnknownType:
		return &unknownImpl{}, nil
	case NullType:
		return &nullImpl{}, nil
	case Int8Type:
		return &int8Impl{}, nil
	case Int16Type:
		return &int16Impl{}, nil
	case Int24Type:
		return &int24Impl{}, nil
	case Int32Type:
		return &int32Impl{}, nil
	case Int64Type:
		return &int64Impl{}, nil
	case Uint8Type:
		return &uint8Impl{}, nil
	case Uint16Type:
		return &uint16Impl{}, nil
	case Uint24Type:
		return &uint24Impl{}, nil
	case Uint32Type:
		return &uint32Impl{}, nil
	case Uint64Type:
		return &uint64Impl{}, nil
	case Float32Type:
		return &float32Impl{}, nil
	case Float64Type:
		return &float64Impl{}, nil
	default:
		return nil, fmt.Errorf("")
	}
}

// DefaultTypeInfo returns the default TypeInfo for a given types.Value.
func DefaultTypeInfo(kind types.NomsKind) (TypeInfo, error) {
	switch kind {
	case types.BlobKind:
		return &unknownImpl{}, nil
	case types.BoolKind:
		return &unknownImpl{}, nil
	case types.FloatKind:
		return &float64Impl{}, nil
	case types.StringKind:
		return &unknownImpl{}, nil
	case types.UUIDKind:
		return &unknownImpl{}, nil
	case types.IntKind:
		return &int64Impl{}, nil
	case types.UintKind:
		return &uint64Impl{}, nil
	case types.NullKind:
		return &nullImpl{}, nil
	case types.InlineBlobKind:
		return &unknownImpl{}, nil
	case types.TimestampKind:
		return &unknownImpl{}, nil
	default:
		return nil, fmt.Errorf(`no default type info for NomsKind "%v"`, kind.String())
	}
}

func ParseIdentifier(name string) Identifier {
	id := Identifier(name)
	_, ok := Identifiers[id]
	if ok {
		return id
	}
	return UnknownType
}

// String returns a string representation of the identifier. This may later be used in parsing to
// retrieve the original identifier.
func (i Identifier) String() string {
	return string(i)
}
