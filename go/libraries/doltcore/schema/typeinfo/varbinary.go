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
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	varBinaryTypeParam_Length        = "length"
	varBinaryTypeParam_SQL           = "sql"
	varBinaryTypeParam_SQL_Binary    = "bin"
	varBinaryTypeParam_SQL_VarBinary = "varbin"
	varBinaryTypeParam_SQL_Blob      = "blob"
)

// As a type, this is modeled more after MySQL's story for binary data. There, it's treated
// as a string that is interpreted as raw bytes, rather than as a bespoke data structure,
// and thus this is mirrored here in its implementation. This will minimize any differences
// that could arise.
type varBinaryType struct {
	sqlBinaryType sql.StringType
}

var _ TypeInfo = (*varBinaryType)(nil)

func CreateVarBinaryTypeFromParams(params map[string]string) (TypeInfo, error) {
	var length int64
	var err error
	if lengthStr, ok := params[varBinaryTypeParam_Length]; ok {
		length, err = strconv.ParseInt(lengthStr, 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create varbinary type info is missing param "%v"`, varBinaryTypeParam_Length)
	}
	if sqlStr, ok := params[varBinaryTypeParam_SQL]; ok {
		var sqlType sql.StringType
		switch sqlStr {
		case varBinaryTypeParam_SQL_Binary:
			sqlType, err = sql.CreateBinary(sqltypes.Binary, length)
		case varBinaryTypeParam_SQL_VarBinary:
			sqlType, err = sql.CreateBinary(sqltypes.VarBinary, length)
		case varBinaryTypeParam_SQL_Blob:
			sqlType, err = sql.CreateBinary(sqltypes.Blob, length)
		default:
			return nil, fmt.Errorf(`create varbinary type info has "%v" param with value "%v"`, varBinaryTypeParam_SQL, sqlStr)
		}
		if err != nil {
			return nil, err
		}
		return &varBinaryType{sqlType}, nil
	} else {
		return nil, fmt.Errorf(`create varbinary type info is missing param "%v"`, varBinaryTypeParam_SQL)
	}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *varBinaryType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.String); ok {
		return ti.sqlBinaryType.Convert(string(val))
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *varBinaryType) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlBinaryType.Convert(v)
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
func (ti *varBinaryType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*varBinaryType); ok {
		return ti.sqlBinaryType.MaxCharacterLength() == ti2.sqlBinaryType.MaxCharacterLength() &&
			ti.sqlBinaryType.Type() == ti2.sqlBinaryType.Type()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *varBinaryType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.String); ok {
		res, err := ti.sqlBinaryType.Convert(string(val))
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
func (ti *varBinaryType) GetTypeIdentifier() Identifier {
	return VarBinaryTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *varBinaryType) GetTypeParams() map[string]string {
	typeParams := map[string]string{
		varBinaryTypeParam_Length: strconv.FormatInt(ti.sqlBinaryType.MaxCharacterLength(), 10),
	}
	switch ti.sqlBinaryType.Type() {
	case sqltypes.Binary:
		typeParams[varBinaryTypeParam_SQL] = varBinaryTypeParam_SQL_Binary
	case sqltypes.VarBinary:
		typeParams[varBinaryTypeParam_SQL] = varBinaryTypeParam_SQL_VarBinary
	case sqltypes.Blob:
		typeParams[varBinaryTypeParam_SQL] = varBinaryTypeParam_SQL_Blob
	default:
		panic(fmt.Errorf(`unknown varbinary type info sql type "%v"`, ti.sqlBinaryType.Type().String()))
	}
	return typeParams
}

// IsValid implements TypeInfo interface.
func (ti *varBinaryType) IsValid(v types.Value) bool {
	_, err := ti.ConvertNomsValueToValue(v)
	return err == nil
}

// NomsKind implements TypeInfo interface.
func (ti *varBinaryType) NomsKind() types.NomsKind {
	return types.StringKind
}

// ParseValue implements TypeInfo interface.
func (ti *varBinaryType) ParseValue(str *string) (types.Value, error) {
	if str == nil {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlBinaryType.Convert(*str)
	if err != nil {
		return nil, err
	}
	if val, ok := strVal.(string); ok {
		return types.String(val), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert the string "%v" to a value`, ti.String(), str)
}

// String implements TypeInfo interface.
func (ti *varBinaryType) String() string {
	sqlType := ""
	switch ti.sqlBinaryType.Type() {
	case sqltypes.Binary:
		sqlType = "Binary"
	case sqltypes.VarBinary:
		sqlType = "VarBinary"
	case sqltypes.Blob:
		sqlType = "Blob"
	default:
		panic(fmt.Errorf(`unknown varbinary type info sql type "%v"`, ti.sqlBinaryType.Type().String()))
	}
	return fmt.Sprintf(`VarBinary(%v, SQL: %v)`, ti.sqlBinaryType.MaxCharacterLength(), sqlType)
}

// ToSqlType implements TypeInfo interface.
func (ti *varBinaryType) ToSqlType() sql.Type {
	return ti.sqlBinaryType
}
