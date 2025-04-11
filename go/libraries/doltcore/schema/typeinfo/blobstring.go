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

package typeinfo

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	blobStringTypeParam_Collate = "collate"
	blobStringTypeParam_Length  = "length"
)

// blobStringType handles the TEXT types. This was originally done in varStringType, however it did not properly
// handle large strings (such as strings over several hundred megabytes), and thus this type was created. Any
// repositories that were made before the introduction of blobStringType will still use varStringType for existing
// columns.
type blobStringType struct {
	sqlStringType sql.StringType
}

var _ TypeInfo = (*blobStringType)(nil)

var (
	TinyTextType   TypeInfo = &blobStringType{sqlStringType: gmstypes.TinyText}
	TextType       TypeInfo = &blobStringType{sqlStringType: gmstypes.Text}
	MediumTextType TypeInfo = &blobStringType{sqlStringType: gmstypes.MediumText}
	LongTextType   TypeInfo = &blobStringType{sqlStringType: gmstypes.LongText}
)

func CreateBlobStringTypeFromParams(params map[string]string) (TypeInfo, error) {
	collationStr, ok := params[blobStringTypeParam_Collate]
	if !ok {
		return nil, fmt.Errorf(`create blobstring type info is missing param "%v"`, blobStringTypeParam_Collate)
	}
	collation, err := sql.ParseCollation("", collationStr, false)
	if err != nil {
		return nil, err
	}

	maxLengthStr, ok := params[blobStringTypeParam_Length]
	if !ok {
		return nil, fmt.Errorf(`create blobstring type info is missing param "%v"`, blobStringTypeParam_Length)
	}
	length, err := strconv.ParseInt(maxLengthStr, 10, 64)
	if err != nil {
		return nil, err
	}

	sqlType, err := gmstypes.CreateString(sqltypes.Text, length, collation)
	if err != nil {
		return nil, err
	}
	return &blobStringType{sqlType}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *blobStringType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Blob); ok {
		b, err := fromBlob(val)
		if gmstypes.IsBinaryType(ti.sqlStringType) {
			return b, err
		}
		return string(b), err
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *blobStringType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.PeekKind()
	switch k {
	case types.BlobKind:
		val, err := reader.ReadBlob()
		if err != nil {
			return nil, err
		}
		b, err := fromBlob(val)
		if gmstypes.IsBinaryType(ti.sqlStringType) {
			return b, err
		}
		return string(b), err
	case types.NullKind:
		_ = reader.ReadKind()
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *blobStringType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	strVal, _, err := ti.sqlStringType.Convert(v)
	if err != nil {
		return nil, err
	}
	val, ok := strVal.(string)
	if ok && utf8.ValidString(val) { // We need to move utf8 (collation) validation into the server
		return types.NewBlob(ctx, vrw, strings.NewReader(val))
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *blobStringType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*blobStringType); ok {
		return ti.sqlStringType.MaxCharacterLength() == ti2.sqlStringType.MaxCharacterLength() &&
			ti.sqlStringType.Collation().Equals(ti2.sqlStringType.Collation())
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *blobStringType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.Blob); ok {
		resStr, err := fromBlob(val)
		if err != nil {
			return nil, err
		}
		return (*string)(unsafe.Pointer(&resStr)), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *blobStringType) GetTypeIdentifier() Identifier {
	return BlobStringTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *blobStringType) GetTypeParams() map[string]string {
	return map[string]string{
		blobStringTypeParam_Collate: ti.sqlStringType.Collation().String(),
		blobStringTypeParam_Length:  strconv.FormatInt(ti.sqlStringType.MaxCharacterLength(), 10),
	}
}

// IsValid implements TypeInfo interface.
func (ti *blobStringType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Blob); ok {
		if int64(val.Len()) <= ti.sqlStringType.MaxByteLength() {
			return true
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *blobStringType) NomsKind() types.NomsKind {
	return types.BlobKind
}

// Promote implements TypeInfo interface.
func (ti *blobStringType) Promote() TypeInfo {
	return &blobStringType{ti.sqlStringType.Promote().(sql.StringType)}
}

// String implements TypeInfo interface.
func (ti *blobStringType) String() string {
	return fmt.Sprintf(`BlobString(%v, %v)`, ti.sqlStringType.Collation().String(), ti.sqlStringType.MaxCharacterLength())
}

// ToSqlType implements TypeInfo interface.
func (ti *blobStringType) ToSqlType() sql.Type {
	return ti.sqlStringType
}

// blobStringTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func blobStringTypeConverter(ctx context.Context, src *blobStringType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			blob, ok := v.(types.Blob)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting blob to %s: %T", strings.ToLower(dest.String()), v)
			}
			val, err := fromBlob(blob)
			if err != nil {
				return nil, err
			}
			newVal, err := strconv.ParseUint(string(val), 10, int(dest.sqlBitType.NumberOfBits()))
			if err != nil {
				return nil, err
			}
			return types.Uint(newVal), nil
		}, true, nil
	case *blobStringType:
		return wrapIsValid(dest.IsValid, src, dest)
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
	case *geomcollType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *geometryType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *inlineBlobType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *intType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *jsonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *linestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multilinestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multipointType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multipolygonType:
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
		return wrapIsValid(dest.IsValid, src, dest)
	case *varStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *yearType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}
