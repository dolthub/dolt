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
	"context"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	inlineBlobTypeParam_Length        = "length"
	inlineBlobTypeParam_SQL           = "sql"
	inlineBlobTypeParam_SQL_Binary    = "bin"
	inlineBlobTypeParam_SQL_VarBinary = "varbin"
)

// inlineBlobType handles BINARY and VARBINARY. BLOB types are handled by varBinaryType.
type inlineBlobType struct {
	sqlBinaryType sql.StringType
}

var _ TypeInfo = (*inlineBlobType)(nil)

func CreateInlineBlobTypeFromParams(params map[string]string) (TypeInfo, error) {
	var length int64
	var err error
	if lengthStr, ok := params[inlineBlobTypeParam_Length]; ok {
		length, err = strconv.ParseInt(lengthStr, 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create inlineblob type info is missing param "%v"`, inlineBlobTypeParam_Length)
	}
	if sqlStr, ok := params[inlineBlobTypeParam_SQL]; ok {
		var sqlType sql.StringType
		switch sqlStr {
		case inlineBlobTypeParam_SQL_Binary:
			sqlType, err = sql.CreateBinary(sqltypes.Binary, length)
		case inlineBlobTypeParam_SQL_VarBinary:
			sqlType, err = sql.CreateBinary(sqltypes.VarBinary, length)
		default:
			return nil, fmt.Errorf(`create inlineblob type info has "%v" param with value "%v"`, inlineBlobTypeParam_SQL, sqlStr)
		}
		if err != nil {
			return nil, err
		}
		return &inlineBlobType{sqlType}, nil
	} else {
		return nil, fmt.Errorf(`create inlineblob type info is missing param "%v"`, inlineBlobTypeParam_SQL)
	}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *inlineBlobType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.InlineBlob); ok {
		return *(*string)(unsafe.Pointer(&val)), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *inlineBlobType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.InlineBlobKind:
		bytes := reader.ReadInlineBlob()
		return *(*string)(unsafe.Pointer(&bytes)), nil
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *inlineBlobType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlBinaryType.Convert(v)
	if err != nil {
		return nil, err
	}
	val, ok := strVal.(string)
	if ok {
		return *(*types.InlineBlob)(unsafe.Pointer(&val)), nil
	}
	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
}

// Equals implements TypeInfo interface.
func (ti *inlineBlobType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*inlineBlobType); ok {
		return ti.sqlBinaryType.MaxCharacterLength() == ti2.sqlBinaryType.MaxCharacterLength() &&
			ti.sqlBinaryType.Type() == ti2.sqlBinaryType.Type()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *inlineBlobType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.InlineBlob); ok {
		convVal, err := ti.ConvertNomsValueToValue(val)
		if err != nil {
			return nil, err
		}
		res, ok := convVal.(string)
		if !ok {
			return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
		}
		return &res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *inlineBlobType) GetTypeIdentifier() Identifier {
	return InlineBlobTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *inlineBlobType) GetTypeParams() map[string]string {
	typeParams := map[string]string{
		inlineBlobTypeParam_Length: strconv.FormatInt(ti.sqlBinaryType.MaxCharacterLength(), 10),
	}
	switch ti.sqlBinaryType.Type() {
	case sqltypes.Binary:
		typeParams[inlineBlobTypeParam_SQL] = inlineBlobTypeParam_SQL_Binary
	case sqltypes.VarBinary:
		typeParams[inlineBlobTypeParam_SQL] = inlineBlobTypeParam_SQL_VarBinary
	default:
		panic(fmt.Errorf(`unknown inlineblob type info sql type "%v"`, ti.sqlBinaryType.Type().String()))
	}
	return typeParams
}

// IsValid implements TypeInfo interface.
func (ti *inlineBlobType) IsValid(v types.Value) bool {
	if val, ok := v.(types.InlineBlob); ok {
		return int64(len(val)) <= ti.sqlBinaryType.MaxByteLength()
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *inlineBlobType) NomsKind() types.NomsKind {
	return types.InlineBlobKind
}

// Promote implements TypeInfo interface.
func (ti *inlineBlobType) Promote() TypeInfo {
	return &inlineBlobType{ti.sqlBinaryType.Promote().(sql.StringType)}
}

// String implements TypeInfo interface.
func (ti *inlineBlobType) String() string {
	sqlType := ""
	switch ti.sqlBinaryType.Type() {
	case sqltypes.Binary:
		sqlType = "Binary"
	case sqltypes.VarBinary:
		sqlType = "VarBinary"
	default:
		panic(fmt.Errorf(`unknown inlineblob type info sql type "%v"`, ti.sqlBinaryType.Type().String()))
	}
	return fmt.Sprintf(`InlineBlob(%v, SQL: %v)`, ti.sqlBinaryType.MaxCharacterLength(), sqlType)
}

// ToSqlType implements TypeInfo interface.
func (ti *inlineBlobType) ToSqlType() sql.Type {
	return ti.sqlBinaryType
}

// inlineBlobTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func inlineBlobTypeConverter(ctx context.Context, src *inlineBlobType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			inlineBlob, ok := v.(types.InlineBlob)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting inlineblob to %s: %T", strings.ToLower(dest.String()), v)
			}
			val := *(*string)(unsafe.Pointer(&inlineBlob))
			newVal, err := strconv.ParseUint(val, 10, int(dest.sqlBitType.NumberOfBits()))
			if err != nil {
				return nil, err
			}
			return types.Uint(newVal), nil
		}, true, nil
	case *blobStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *boolType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *datetimeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *decimalType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *enumType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *floatType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *inlineBlobType:
		return wrapIsValid(dest.IsValid, src, dest)
	case *intType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *jsonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *linestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *pointType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *polygonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *setType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *timeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *uintType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *uuidType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *varBinaryType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *varStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *yearType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}
