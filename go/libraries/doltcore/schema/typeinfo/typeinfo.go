// Copyright 2020 Dolthub, Inc.
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
	"os"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// UseAdaptiveEncoding indicates whether to use adaptive encoding for large/unbounded fields instead of address
// encoding. Tests can set this variable to true in order to force Dolt to use adaptive encoding for TEXT and BLOB
// columns. Extended types will always use adaptive encoding for TEXT and BLOB types regardless of this value.
var UseAdaptiveEncoding = true

func init() {
	if envVal, ok := os.LookupEnv("DOLT_USE_ADAPTIVE_ENCODING"); ok && envVal == "false" {
		UseAdaptiveEncoding = false
	}
}

// TypeInfo is an interface used for encoding type information.
type TypeInfo interface {
	// Equals returns whether the given TypeInfo is equivalent to this TypeInfo.
	Equals(other TypeInfo) bool

	// NomsKind returns the NomsKind that best matches this TypeInfo.
	NomsKind() types.NomsKind

	// ToSqlType returns the TypeInfo as a sql.Type. If an exact match is able to be made then that is
	// the one returned, otherwise the sql.Type is the closest match possible.
	ToSqlType() sql.Type

	// Encoding returns the val.Encoding to use for serializing values of this type.
	Encoding() val.Encoding

	// WithEncoding returns a TypeInfo that serializes values using the given encoding. For most types,
	// the encoding is fixed and this returns the receiver unchanged. For types with variable encodings
	// (e.g. TEXT, BLOB), this returns a new TypeInfo that uses the given encoding.
	WithEncoding(enc val.Encoding) TypeInfo

	// Stringer results are used to inform users of the constraint's properties.
	fmt.Stringer
}

// FromSqlType takes in a sql.Type and returns the most relevant TypeInfo.
func FromSqlType(sqlType sql.Type) (TypeInfo, error) {
	if gmsExtendedType, ok := sqlType.(sql.ExtendedType); ok {
		return CreateExtendedTypeFromSqlType(gmsExtendedType), nil
	}
	sqlType, err := fillInCollationWithDefault(sqlType)
	if err != nil {
		return nil, err
	}

	queryType := sqlType.Type()
	switch queryType {
	case sqltypes.Null:
		return UnknownType, nil
	case sqltypes.Int8:
		// MySQL allows only the TINYINT type to have a display width, so it's the only
		// integer type that needs to be checked for it's underlying NumberType data.
		numberType, ok := sqlType.(sql.NumberType)
		if !ok {
			return nil, fmt.Errorf("expected sql.NumberType, but received: %T", sqlType)
		}
		return &intType{sqlIntType: numberType}, nil
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
	case sqltypes.Timestamp, sqltypes.Datetime:
		return CreateDatetimeTypeFromSqlType(sqlType.(sql.DatetimeType)), nil
	case sqltypes.Date:
		return DateType, nil
	case sqltypes.Time:
		return TimeType, nil
	case sqltypes.Year:
		return YearType, nil
	case sqltypes.Geometry:
		switch sqlType.String() {
		case gmstypes.PointType{}.String():
			return &pointType{sqlPointType: sqlType.(gmstypes.PointType)}, nil
		case gmstypes.LineStringType{}.String():
			return &linestringType{sqlLineStringType: sqlType.(gmstypes.LineStringType)}, nil
		case gmstypes.PolygonType{}.String():
			return &polygonType{sqlPolygonType: sqlType.(gmstypes.PolygonType)}, nil
		case gmstypes.MultiPointType{}.String():
			return &multipointType{sqlMultiPointType: sqlType.(gmstypes.MultiPointType)}, nil
		case gmstypes.MultiLineStringType{}.String():
			return &multilinestringType{sqlMultiLineStringType: sqlType.(gmstypes.MultiLineStringType)}, nil
		case gmstypes.MultiPolygonType{}.String():
			return &multipolygonType{sqlMultiPolygonType: sqlType.(gmstypes.MultiPolygonType)}, nil
		case gmstypes.GeomCollType{}.String():
			return &geomcollType{sqlGeomCollType: sqlType.(gmstypes.GeomCollType)}, nil
		case gmstypes.GeometryType{}.String():
			return &geometryType{sqlGeometryType: sqlType.(gmstypes.GeometryType)}, nil
		default:
			return nil, fmt.Errorf(`expected "PointTypeIdentifier" from SQL basetype "Geometry"`)
		}
	case sqltypes.Decimal:
		decimalSQLType, ok := sqlType.(sql.DecimalType)
		if !ok {
			return nil, fmt.Errorf(`expected "DecimalTypeIdentifier" from SQL basetype "Decimal"`)
		}
		return &decimalType{sqlDecimalType: decimalSQLType}, nil
	case sqltypes.Text:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Text"`)
		}
		return &blobStringType{sqlStringType: stringType}, nil
	case sqltypes.Blob:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Blob"`)
		}
		return &varBinaryType{sqlBinaryType: stringType}, nil
	case sqltypes.VarChar:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "VarChar"`)
		}
		return &varStringType{sqlStringType: stringType}, nil
	case sqltypes.VarBinary:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "VarBinary"`)
		}
		return &inlineBlobType{sqlBinaryType: stringType}, nil
	case sqltypes.Char:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Char"`)
		}
		return &varStringType{sqlStringType: stringType}, nil
	case sqltypes.Binary:
		stringType, ok := sqlType.(sql.StringType)
		if !ok {
			return nil, fmt.Errorf(`expected "StringType" from SQL basetype "Binary"`)
		}
		return &inlineBlobType{sqlBinaryType: stringType}, nil
	case sqltypes.Bit:
		bitSQLType, ok := sqlType.(gmstypes.BitType)
		if !ok {
			return nil, fmt.Errorf(`expected "BitTypeIdentifier" from SQL basetype "Bit"`)
		}
		return &bitType{sqlBitType: bitSQLType}, nil
	case sqltypes.TypeJSON:
		js, ok := sqlType.(gmstypes.JsonType)
		if !ok {
			return nil, fmt.Errorf(`expected "JsonType" from SQL basetype "TypeJSON"`)
		}
		return &jsonType{jsonType: js}, nil
	case sqltypes.Enum:
		enumSQLType, ok := sqlType.(sql.EnumType)
		if !ok {
			return nil, fmt.Errorf(`expected "EnumTypeIdentifier" from SQL basetype "Enum"`)
		}
		return &enumType{sqlEnumType: enumSQLType}, nil
	case sqltypes.Set:
		setSQLType, ok := sqlType.(sql.SetType)
		if !ok {
			return nil, fmt.Errorf(`expected "SetTypeIdentifier" from SQL basetype "Set"`)
		}
		return &setType{sqlSetType: setSQLType}, nil
	case sqltypes.Vector:
		vectorSQLType, ok := sqlType.(gmstypes.VectorType)
		if !ok {
			return nil, fmt.Errorf(`expected "VectorTypeIdentifier" from SQL basetype "Vector"`)
		}
		return &vectorType{sqlVectorType: vectorSQLType}, nil
	default:
		return nil, fmt.Errorf(`no type info can be created from SQL base type "%v"`, sqlType.String())
	}
}

// fillInCollationWithDefault sets the default collation on any collated type with no collation set.
// We don't serialize the collation if it's the default collation, but the engine expects every column to have an
// explicit collation. So fill it in on load in that case.
func fillInCollationWithDefault(typ sql.Type) (sql.Type, error) {
	if collationType, ok := typ.(sql.TypeWithCollation); ok && collationType.Collation() == sql.Collation_Unspecified {
		return collationType.WithNewCollation(sql.Collation_Default)
	}
	return typ, nil
}

// FromKind returns the default TypeInfo for a given types.Value.
// Deprecated. Use FromSqlType instead.
func FromKind(kind types.NomsKind) TypeInfo {
	switch kind {
	case types.BlobKind:
		return &varBinaryType{sqlBinaryType: gmstypes.LongBlob}
	case types.BoolKind:
		return BoolType
	case types.ExtendedKind:
		panic(fmt.Errorf(`type not supported by the old format "%v"`, kind.String()))
	case types.FloatKind:
		return Float64Type
	case types.InlineBlobKind:
		return &inlineBlobType{sqlBinaryType: gmstypes.MustCreateBinary(sqltypes.VarBinary, MaxVarcharLength/16)}
	case types.IntKind:
		return Int64Type
	case types.JSONKind:
		return JSONType
	case types.LineStringKind:
		return LineStringType
	case types.NullKind:
		return UnknownType
	case types.GeometryKind:
		return GeometryType
	case types.PointKind:
		return PointType
	case types.PolygonKind:
		return PolygonType
	case types.StringKind:
		return StringDefaultType
	case types.TimestampKind:
		return DatetimeType
	case types.UintKind:
		return Uint64Type
	case types.UUIDKind:
		return UuidType
	case types.DecimalKind:
		return &decimalType{sqlDecimalType: gmstypes.MustCreateDecimalType(65, 30)}
	default:
		panic(fmt.Errorf(`no default type info for NomsKind "%v"`, kind.String()))
	}
}
