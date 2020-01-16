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
	UnknownType    Identifier = "unknown"
	BitType        Identifier = "bit"
	BoolType       Identifier = "bool"
	DatetimeType   Identifier = "datetime"
	DecimalType    Identifier = "decimal"
	EnumType       Identifier = "enum"
	FloatType      Identifier = "float"
	InlineBlobType Identifier = "inlineblob"
	IntType        Identifier = "int"
	SetType        Identifier = "set"
	TimeType       Identifier = "time"
	UintType       Identifier = "uint"
	UuidType       Identifier = "uuid"
	VarBinaryType  Identifier = "varbinary"
	VarStringType  Identifier = "varstring"
	YearType       Identifier = "year"
)

var Identifiers = map[Identifier]struct{}{
	UnknownType:    {},
	BitType:        {},
	BoolType:       {},
	DatetimeType:   {},
	DecimalType:    {},
	EnumType:       {},
	FloatType:      {},
	IntType:        {},
	InlineBlobType: {},
	SetType:        {},
	TimeType:       {},
	UintType:       {},
	UuidType:       {},
	VarBinaryType:  {},
	VarStringType:  {},
	YearType:       {},
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

// TypeInfoFromSqlType takes in a sql.Type and returns the relevant TypeInfo.
func TypeInfoFromSqlType(sqlType sql.Type) (TypeInfo, error) {
	switch sqlType.Type() {
	case sqltypes.Int8:
		return &intImpl{IntWidth8}, nil
	case sqltypes.Int16:
		return &intImpl{IntWidth16}, nil
	case sqltypes.Int24:
		return &intImpl{IntWidth24}, nil
	case sqltypes.Int32:
		return &intImpl{IntWidth32}, nil
	case sqltypes.Int64:
		return &intImpl{IntWidth64}, nil
	case sqltypes.Uint8:
		return &uintImpl{UintWidth8}, nil
	case sqltypes.Uint16:
		return &uintImpl{UintWidth16}, nil
	case sqltypes.Uint24:
		return &uintImpl{UintWidth24}, nil
	case sqltypes.Uint32:
		return &uintImpl{UintWidth32}, nil
	case sqltypes.Uint64:
		return &uintImpl{UintWidth64}, nil
	case sqltypes.Float32:
		return &floatImpl{FloatWidth32}, nil
	case sqltypes.Float64:
		return &floatImpl{FloatWidth64}, nil
	case sqltypes.Timestamp:
		datetimeType, ok := sqlType.(sql.DatetimeType)
		if !ok {
			return nil, fmt.Errorf(`expected "DatetimeType" from SQL basetype "Timestamp"`)
		}
		return &datetimeImpl{
			Min:      datetimeType.MinimumTime(),
			Max:      datetimeType.MaximumTime(),
			DateOnly: false,
		}, nil
	case sqltypes.Date:
		datetimeType, ok := sqlType.(sql.DatetimeType)
		if !ok {
			return nil, fmt.Errorf(`expected "DatetimeType" from SQL basetype "Date"`)
		}
		return &datetimeImpl{
			Min:      datetimeType.MinimumTime(),
			Max:      datetimeType.MaximumTime(),
			DateOnly: true,
		}, nil
	case sqltypes.Time:
		return &timeImpl{}, nil
	case sqltypes.Datetime:
		datetimeType, ok := sqlType.(sql.DatetimeType)
		if !ok {
			return nil, fmt.Errorf(`expected "DatetimeType" from SQL basetype "Datetime"`)
		}
		return &datetimeImpl{
			Min:      datetimeType.MinimumTime(),
			Max:      datetimeType.MaximumTime(),
			DateOnly: false,
		}, nil
	case sqltypes.Year:
		return &yearImpl{}, nil
	case sqltypes.Decimal:
		decimalType, ok := sqlType.(sql.DecimalType)
		if !ok {
			return nil, fmt.Errorf(`expected "DecimalType" from SQL basetype "Decimal"`)
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
		}, nil
	case sqltypes.Blob:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Blob"`)
		}
		return &varBinaryImpl{stringType.MaxByteLength(), false}, nil
	case sqltypes.VarChar:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "VarChar"`)
		}
		return &varStringImpl{
			stringType.Collation(),
			stringType.MaxCharacterLength(),
			false,
		}, nil
	case sqltypes.VarBinary:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "VarBinary"`)
		}
		return &varBinaryImpl{stringType.MaxByteLength(), false}, nil
	case sqltypes.Char:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Char"`)
		}
		return &varStringImpl{
			stringType.Collation(),
			stringType.MaxCharacterLength(),
			true,
		}, nil
	case sqltypes.Binary:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Binary"`)
		}
		return &varBinaryImpl{stringType.MaxByteLength(), true}, nil
	case sqltypes.Bit:
		bitType, ok := sqlType.(sql.BitType)
		if !ok {
			return nil, fmt.Errorf(`expected "BitType" from SQL basetype "Bit"`)
		}
		return &bitImpl{bitType}, nil
	case sqltypes.Enum:
		enumType, ok := sqlType.(sql.EnumType)
		if !ok {
			return nil, fmt.Errorf(`expected "EnumType" from SQL basetype "Enum"`)
		}
		return &enumImpl{enumType}, nil
	case sqltypes.Set:
		setType, ok := sqlType.(sql.SetType)
		if !ok {
			return nil, fmt.Errorf(`expected "SetType" from SQL basetype "Set"`)
		}
		return &setImpl{setType}, nil
	default:
		return nil, fmt.Errorf(`no type info can be created from SQL base type "%v"`, sqlType.String())
	}
}

// TypeInfoFromIdentifierParams constructs a TypeInfo from the given identifier and parameters.
func TypeInfoFromIdentifierParams(id Identifier, params map[string]string) (TypeInfo, error) {
	switch id {
	case BitType:
		return CreateBitType(params)
	case BoolType:
		return CreateBoolType(params)
	case DatetimeType:
		return CreateDatetimeType(params)
	case DecimalType:
		return CreateDecimalType(params)
	case EnumType:
		return CreateEnumType(params)
	case FloatType:
		return CreateFloatType(params)
	case IntType:
		return CreateIntType(params)
	case InlineBlobType:
		return CreateInlineBlobType(params)
	case SetType:
		return CreateSetType(params)
	case TimeType:
		return CreateTimeType(params)
	case UintType:
		return CreateUintType(params)
	case UuidType:
		return CreateUuidType(params)
	case VarBinaryType:
		return CreateVarBinaryType(params)
	case VarStringType:
		return CreateVarStringType(params)
	case YearType:
		return CreateYearType(params)
	default:
		return nil, fmt.Errorf(`"%v" cannot be made from an identifier and params`, id)
	}
}

// DefaultTypeInfo returns the default TypeInfo for a given types.Value.
func DefaultTypeInfo(kind types.NomsKind) TypeInfo {
	switch kind {
	case types.BoolKind:
		return &boolImpl{}
	case types.FloatKind:
		return &floatImpl{FloatWidth64}
	case types.StringKind:
		return &varStringImpl{
			sql.Collation_Default,
			1<<32-1,
			false,
		}
	case types.UUIDKind:
		return &uuidImpl{}
	case types.IntKind:
		return &intImpl{IntWidth64}
	case types.UintKind:
		return &uintImpl{UintWidth64}
	case types.InlineBlobKind:
		return &inlineBlobImpl{}
	case types.TimestampKind:
		// Here we set it to the limits of the SQL Datetime type just so conversions
		// between the two types are straightforward. This is an arbitrary decision and
		// this can definitely be widened later if we decide to.
		return &datetimeImpl{
			sql.Datetime.MinimumTime(),
			sql.Datetime.MaximumTime(),
			false,
		}
	default:
		panic(fmt.Errorf(`no default type info for NomsKind "%v"`, kind.String()))
	}
}

// ParseIdentifier takes in an Identifier in string form and returns the matching Identifier.
// Returns UnknownType when the string match is not found.
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
