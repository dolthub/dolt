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
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	varStringTypeParam_Collate     = "collate"
	varStringTypeParam_Length      = "length"
	varStringTypeParam_SQL         = "sql"
	varStringTypeParam_SQL_Char    = "char"
	varStringTypeParam_SQL_VarChar = "varchar"
	varStringTypeParam_SQL_Text    = "text"
)

// varStringType handles CHAR and VARCHAR. The TEXT types are handled by blobStringType. For any repositories that were
// created before the introduction of blobStringType, they will use varStringType for TEXT types. As varStringType makes
// use of the String Value type, it does not actually support all viable lengths of a TEXT string, meaning all such
// legacy repositories will run into issues if they attempt to insert very large strings. Any and all new repositories
// must use blobStringType for all TEXT types to ensure proper behavior.
type varStringType struct {
	sqlStringType sql.StringType
}

var _ TypeInfo = (*varStringType)(nil)

var (
	LegacyStringDefaultType = &varStringType{sql.CreateLongText(sql.Collation_Default)}
	StringDefaultType       = &varStringType{sql.MustCreateStringWithDefaults(sqltypes.VarChar, 16383)}
)

func CreateVarStringTypeFromParams(params map[string]string) (TypeInfo, error) {
	var length int64
	var collation sql.Collation
	var err error
	if collationStr, ok := params[varStringTypeParam_Collate]; ok {
		collation, err = sql.ParseCollation(nil, &collationStr, false)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create varstring type info is missing param "%v"`, varStringTypeParam_Collate)
	}
	if maxLengthStr, ok := params[varStringTypeParam_Length]; ok {
		length, err = strconv.ParseInt(maxLengthStr, 10, 64)
		if err != nil {
			return nil, err
		}

	} else {
		return nil, fmt.Errorf(`create varstring type info is missing param "%v"`, varStringTypeParam_Length)
	}
	if sqlStr, ok := params[varStringTypeParam_SQL]; ok {
		var sqlType sql.StringType
		switch sqlStr {
		case varStringTypeParam_SQL_Char:
			sqlType, err = sql.CreateString(sqltypes.Char, length, collation)
		case varStringTypeParam_SQL_VarChar:
			sqlType, err = sql.CreateString(sqltypes.VarChar, length, collation)
		case varStringTypeParam_SQL_Text:
			sqlType, err = sql.CreateString(sqltypes.Text, length, collation)
		default:
			return nil, fmt.Errorf(`create varstring type info has "%v" param with value "%v"`, varStringTypeParam_SQL, sqlStr)
		}
		if err != nil {
			return nil, err
		}
		return &varStringType{sqlType}, nil
	} else {
		return nil, fmt.Errorf(`create varstring type info is missing param "%v"`, varStringTypeParam_Length)
	}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *varStringType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.String); ok {
		res := string(val)
		// As per the MySQL documentation, trailing spaces are removed when retrieved for CHAR types only.
		// This function is used to retrieve dolt values, hence its inclusion here and not elsewhere.
		// https://dev.mysql.com/doc/refman/8.0/en/char.html
		if ti.sqlStringType.Type() == sqltypes.Char {
			res = strings.TrimRightFunc(res, unicode.IsSpace)
		}
		return res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *varStringType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.StringKind:
		val := reader.ReadString()
		// As per the MySQL documentation, trailing spaces are removed when retrieved for CHAR types only.
		// This function is used to retrieve dolt values, hence its inclusion here and not elsewhere.
		// https://dev.mysql.com/doc/refman/8.0/en/char.html
		if ti.sqlStringType.Type() == sqltypes.Char {
			val = strings.TrimRightFunc(val, unicode.IsSpace)
		}
		return val, nil

	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *varStringType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlStringType.Convert(v)
	if err != nil {
		return nil, err
	}
	val, ok := strVal.(string)
	if ok {
		return types.String(val), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *varStringType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*varStringType); ok {
		return ti.sqlStringType.MaxCharacterLength() == ti2.sqlStringType.MaxCharacterLength() &&
			ti.sqlStringType.Type() == ti2.sqlStringType.Type() &&
			ti.sqlStringType.Collation().Equals(ti2.sqlStringType.Collation())
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *varStringType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.String); ok {
		res, err := ti.ConvertNomsValueToValue(val)
		if err != nil {
			return nil, err
		}
		if resStr, ok := res.(string); ok {
			return &resStr, nil
		}
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *varStringType) GetTypeIdentifier() Identifier {
	return VarStringTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *varStringType) GetTypeParams() map[string]string {
	typeParams := map[string]string{
		varStringTypeParam_Collate: ti.sqlStringType.Collation().String(),
		varStringTypeParam_Length:  strconv.FormatInt(ti.sqlStringType.MaxCharacterLength(), 10),
	}
	switch ti.sqlStringType.Type() {
	case sqltypes.Char:
		typeParams[varStringTypeParam_SQL] = varStringTypeParam_SQL_Char
	case sqltypes.VarChar:
		typeParams[varStringTypeParam_SQL] = varStringTypeParam_SQL_VarChar
	case sqltypes.Text:
		typeParams[varStringTypeParam_SQL] = varStringTypeParam_SQL_Text
	default:
		panic(fmt.Errorf(`unknown varstring type info sql type "%v"`, ti.sqlStringType.Type().String()))
	}
	return typeParams
}

// IsValid implements TypeInfo interface.
func (ti *varStringType) IsValid(v types.Value) bool {
	if val, ok := v.(types.String); ok {
		_, err := ti.sqlStringType.Convert(string(val))
		if err != nil {
			return false
		}
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *varStringType) NomsKind() types.NomsKind {
	return types.StringKind
}

// Promote implements TypeInfo interface.
func (ti *varStringType) Promote() TypeInfo {
	return &varStringType{ti.sqlStringType.Promote().(sql.StringType)}
}

// String implements TypeInfo interface.
func (ti *varStringType) String() string {
	sqlType := ""
	switch ti.sqlStringType.Type() {
	case sqltypes.Char:
		sqlType = "Char"
	case sqltypes.VarChar:
		sqlType = "VarChar"
	case sqltypes.Text:
		sqlType = "Text"
	default:
		panic(fmt.Errorf(`unknown varstring type info sql type "%v"`, ti.sqlStringType.Type().String()))
	}
	return fmt.Sprintf(`VarString(%v, %v, SQL: %v)`, ti.sqlStringType.Collation().String(), ti.sqlStringType.MaxCharacterLength(), sqlType)
}

// ToSqlType implements TypeInfo interface.
func (ti *varStringType) ToSqlType() sql.Type {
	return ti.sqlStringType
}

// ConvertToType converts the given string to the destination value. This is a shortcut to calling GetTypeConverter and
// using the returned TypeConverter when this typeinfo will be used as the source typeinfo. This essentially replaces
// the old ParseValue function.
func (ti *varStringType) ConvertToType(ctx context.Context, vrw types.ValueReadWriter, destTi TypeInfo, val types.String) (types.Value, error) {
	tc, _, err := varStringTypeConverter(ctx, ti, destTi)
	if err != nil {
		return nil, err
	}
	return tc(ctx, vrw, val)
}

// varStringTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func varStringTypeConverter(ctx context.Context, src *varStringType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.String)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting string to %s: %T", strings.ToLower(dest.String()), v)
			}
			newVal, err := strconv.ParseUint(string(val), 10, int(dest.sqlBitType.NumberOfBits()))
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
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
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
		return wrapIsValid(dest.IsValid, src, dest)
	case *yearType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}
