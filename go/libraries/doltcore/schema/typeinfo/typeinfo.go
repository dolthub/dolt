// Copyright 2020 Liquidata, Inc.
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
	UnknownTypeIdentifier    Identifier = "unknown"
	BitTypeIdentifier        Identifier = "bit"
	BoolTypeIdentifier       Identifier = "bool"
	DatetimeTypeIdentifier   Identifier = "datetime"
	DecimalTypeIdentifier    Identifier = "decimal"
	EnumTypeIdentifier       Identifier = "enum"
	FloatTypeIdentifier      Identifier = "float"
	InlineBlobTypeIdentifier Identifier = "inlineblob"
	IntTypeIdentifier        Identifier = "int"
	NullTypeIdentifier       Identifier = "null"
	SetTypeIdentifier        Identifier = "set"
	TimeTypeIdentifier       Identifier = "time"
	TupleIdentifier          Identifier = "tuple"
	UintTypeIdentifier       Identifier = "uint"
	UuidTypeIdentifier       Identifier = "uuid"
	VarBinaryTypeIdentifier  Identifier = "varbinary"
	VarStringTypeIdentifier  Identifier = "varstring"
	YearTypeIdentifier       Identifier = "year"
)

var Identifiers = map[Identifier]struct{}{
	UnknownTypeIdentifier:    {},
	BitTypeIdentifier:        {},
	BoolTypeIdentifier:       {},
	DatetimeTypeIdentifier:   {},
	DecimalTypeIdentifier:    {},
	EnumTypeIdentifier:       {},
	FloatTypeIdentifier:      {},
	InlineBlobTypeIdentifier: {},
	IntTypeIdentifier:        {},
	NullTypeIdentifier:       {},
	SetTypeIdentifier:        {},
	TimeTypeIdentifier:       {},
	TupleIdentifier:          {},
	UintTypeIdentifier:       {},
	UuidTypeIdentifier:       {},
	VarBinaryTypeIdentifier:  {},
	VarStringTypeIdentifier:  {},
	YearTypeIdentifier:       {},
}

// TypeInfo is an interface used for encoding type information.
type TypeInfo interface {
	// ConvertNomsValueToValue converts a Noms value to a go value. The expected NomsKind of the given
	// parameter is equivalent to the NomsKind returned by this type info.
	ConvertNomsValueToValue(v types.Value) (interface{}, error)

	// ConvertValueToNomsValue converts a go value or Noms value to a Noms value. The type of the Noms
	// value will be equivalent to the NomsKind returned from NomsKind.
	ConvertValueToNomsValue(v interface{}) (types.Value, error)

	// Equals returns whether the given TypeInfo is equivalent to this TypeInfo.
	Equals(other TypeInfo) bool

	// GetTypeIdentifier returns an identifier for this type used for serialization.
	GetTypeIdentifier() Identifier

	// GetTypeParams returns a map[string]string containing the type parameters.  This is used for
	// serialization and deserialization of type information.
	GetTypeParams() map[string]string

	// IsValid takes in a value (go or Noms) and returns whether the value is valid for this type.
	IsValid(v interface{}) bool

	// NomsKind returns the NomsKind that best matches this TypeInfo.
	NomsKind() types.NomsKind

	// ToSqlType returns the TypeInfo as a sql.Type. If an exact match is able to be made then that is
	// the one returned, otherwise the sql.Type is the closest match possible.
	ToSqlType() sql.Type

	// Stringer results are used to inform users of the constraint's properties.
	fmt.Stringer
}

// FromSqlType takes in a sql.Type and returns the most relevant TypeInfo.
func FromSqlType(sqlType sql.Type) (TypeInfo, error) {
	switch sqlType.Type() {
	case sqltypes.Null:
		return NullType, nil
	case sqltypes.Int8:
		return Int8Type, nil
	case sqltypes.Int16:
		return Int16Type, nil
	case sqltypes.Int24:
		return Int24Type, nil
	case sqltypes.Int32:
		return Int32Type, nil
	case sqltypes.Int64:
		return Int64Type, nil
	case sqltypes.Uint8:
		return Uint8Type, nil
	case sqltypes.Uint16:
		return Uint16Type, nil
	case sqltypes.Uint24:
		return Uint24Type, nil
	case sqltypes.Uint32:
		return Uint32Type, nil
	case sqltypes.Uint64:
		return Uint64Type, nil
	case sqltypes.Float32:
		return Float32Type, nil
	case sqltypes.Float64:
		return Float64Type, nil
	case sqltypes.Timestamp:
		datetimeType, ok := sqlType.(sql.DatetimeType)
		if !ok {
			return nil, fmt.Errorf(`expected "DatetimeTypeIdentifier" from SQL basetype "Timestamp"`)
		}
		return &datetimeImpl{
			Min:      datetimeType.MinimumTime(),
			Max:      datetimeType.MaximumTime(),
			DateOnly: false,
		}, nil
	case sqltypes.Date:
		datetimeType, ok := sqlType.(sql.DatetimeType)
		if !ok {
			return nil, fmt.Errorf(`expected "DatetimeTypeIdentifier" from SQL basetype "Date"`)
		}
		return &datetimeImpl{
			Min:      datetimeType.MinimumTime(),
			Max:      datetimeType.MaximumTime(),
			DateOnly: true,
		}, nil
	case sqltypes.Time:
		return TimeType, nil
	case sqltypes.Datetime:
		datetimeType, ok := sqlType.(sql.DatetimeType)
		if !ok {
			return nil, fmt.Errorf(`expected "DatetimeTypeIdentifier" from SQL basetype "Datetime"`)
		}
		return &datetimeImpl{
			Min:      datetimeType.MinimumTime(),
			Max:      datetimeType.MaximumTime(),
			DateOnly: false,
		}, nil
	case sqltypes.Year:
		return YearType, nil
	case sqltypes.Decimal:
		decimalType, ok := sqlType.(sql.DecimalType)
		if !ok {
			return nil, fmt.Errorf(`expected "DecimalTypeIdentifier" from SQL basetype "Decimal"`)
		}
		return &decimalImpl{decimalType}, nil
	case sqltypes.Text:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Text"`)
		}
		return &varStringImpl{
			stringType.Collation(),
			stringType.MaxCharacterLength(),
			false,
			true,
		}, nil
	case sqltypes.Blob:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Blob"`)
		}
		return &varBinaryImpl{stringType.MaxByteLength(), false, true}, nil
	case sqltypes.VarChar:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "VarChar"`)
		}
		return &varStringImpl{
			stringType.Collation(),
			stringType.MaxCharacterLength(),
			false,
			false,
		}, nil
	case sqltypes.VarBinary:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "VarBinary"`)
		}
		return &varBinaryImpl{stringType.MaxByteLength(), false, false}, nil
	case sqltypes.Char:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Char"`)
		}
		return &varStringImpl{
			stringType.Collation(),
			stringType.MaxCharacterLength(),
			true,
			false,
		}, nil
	case sqltypes.Binary:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Binary"`)
		}
		return &varBinaryImpl{stringType.MaxByteLength(), true, false}, nil
	case sqltypes.Bit:
		bitType, ok := sqlType.(sql.BitType)
		if !ok {
			return nil, fmt.Errorf(`expected "BitTypeIdentifier" from SQL basetype "Bit"`)
		}
		return &bitImpl{bitType}, nil
	case sqltypes.Enum:
		enumType, ok := sqlType.(sql.EnumType)
		if !ok {
			return nil, fmt.Errorf(`expected "EnumTypeIdentifier" from SQL basetype "Enum"`)
		}
		return &enumImpl{enumType}, nil
	case sqltypes.Set:
		setType, ok := sqlType.(sql.SetType)
		if !ok {
			return nil, fmt.Errorf(`expected "SetTypeIdentifier" from SQL basetype "Set"`)
		}
		return &setImpl{setType}, nil
	default:
		return nil, fmt.Errorf(`no type info can be created from SQL base type "%v"`, sqlType.String())
	}
}

// FromTypeParams constructs a TypeInfo from the given identifier and parameters.
func FromTypeParams(id Identifier, params map[string]string) (TypeInfo, error) {
	switch id {
	case BitTypeIdentifier:
		return CreateBitTypeFromParams(params)
	case BoolTypeIdentifier:
		return BoolType, nil
	case DatetimeTypeIdentifier:
		return CreateDatetimeTypeFromParams(params)
	case DecimalTypeIdentifier:
		return CreateDecimalTypeFromParams(params)
	case EnumTypeIdentifier:
		return CreateEnumTypeFromParams(params)
	case FloatTypeIdentifier:
		return CreateFloatTypeFromParams(params)
	case InlineBlobTypeIdentifier:
		return InlineBlobType, nil
	case IntTypeIdentifier:
		return CreateIntTypeFromParams(params)
	case NullTypeIdentifier:
		return NullType, nil
	case SetTypeIdentifier:
		return CreateSetTypeFromParams(params)
	case TimeTypeIdentifier:
		return TimeType, nil
	case TupleIdentifier:
		return TupleType, nil
	case UintTypeIdentifier:
		return CreateUintTypeFromParams(params)
	case UuidTypeIdentifier:
		return UuidType, nil
	case VarBinaryTypeIdentifier:
		return CreateVarBinaryTypeFromParams(params)
	case VarStringTypeIdentifier:
		return CreateVarStringTypeFromParams(params)
	case YearTypeIdentifier:
		return YearType, nil
	default:
		return nil, fmt.Errorf(`"%v" cannot be made from an identifier and params`, id)
	}
}

// FromKind returns the default TypeInfo for a given types.Value.
func FromKind(kind types.NomsKind) TypeInfo {
	switch kind {
	case types.BoolKind:
		return BoolType
	case types.FloatKind:
		return Float64Type
	case types.InlineBlobKind:
		return InlineBlobType
	case types.IntKind:
		return Int64Type
	case types.NullKind:
		return NullType
	case types.StringKind:
		return StringDefaultType
	case types.TimestampKind:
		// Here we set it to the limits of the SQL Datetime type just so conversions
		// between the two types are straightforward. This is an arbitrary decision and
		// this can definitely be widened later if we decide to.
		return &datetimeImpl{
			sql.Datetime.MinimumTime(),
			sql.Datetime.MaximumTime(),
			false,
		}
	case types.TupleKind:
		return TupleType
	case types.UintKind:
		return Uint64Type
	case types.UUIDKind:
		return UuidType
	default:
		panic(fmt.Errorf(`no default type info for NomsKind "%v"`, kind.String()))
	}
}

// ParseIdentifier takes in an Identifier in string form and returns the matching Identifier.
// Returns UnknownTypeIdentifier when the string match is not found.
func ParseIdentifier(name string) Identifier {
	id := Identifier(name)
	_, ok := Identifiers[id]
	if ok {
		return id
	}
	return UnknownTypeIdentifier
}

// String returns a string representation of the identifier. This may later be used in parsing to
// retrieve the original identifier.
func (i Identifier) String() string {
	return string(i)
}
