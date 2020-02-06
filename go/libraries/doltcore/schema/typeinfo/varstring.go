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
	"math"
	"strconv"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	"vitess.io/vitess/go/sqltypes"

	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	varStringTypeParam_Collate              = "collate"
	varStringTypeParam_Length               = "length"
	varStringTypeParam_RemoveTrailingSpaces = "rts"
	varStringTypeParam_IsSqlText            = "text"
)

type varStringImpl struct {
	Collation            sql.Collation
	MaxLength            int64
	RemoveTrailingSpaces bool
	IsSqlText            bool // When converting to a SQL type, this ensures a distinction between TEXT and VARCHAR
}

var _ TypeInfo = (*varStringImpl)(nil)
var StringDefaultType TypeInfo = &varStringImpl{
	sql.Collation_Default,
	math.MaxUint32,
	false,
	true,
}

func CreateVarStringTypeFromParams(params map[string]string) (TypeInfo, error) {
	ti := &varStringImpl{}
	var err error
	if collationStr, ok := params[varStringTypeParam_Collate]; ok {
		ti.Collation, err = sql.ParseCollation(nil, &collationStr, false)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create varstring type info is missing param "%v"`, varStringTypeParam_Collate)
	}
	if maxLengthStr, ok := params[varStringTypeParam_Length]; ok {
		ti.MaxLength, err = strconv.ParseInt(maxLengthStr, 10, 64)
	} else {
		return nil, fmt.Errorf(`create varstring type info is missing param "%v"`, varStringTypeParam_Length)
	}
	if _, ok := params[varStringTypeParam_RemoveTrailingSpaces]; ok {
		ti.RemoveTrailingSpaces = true
	}
	if _, ok := params[varStringTypeParam_IsSqlText]; ok {
		ti.IsSqlText = true
	}
	return ti, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *varStringImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.String); ok {
		if ti.RemoveTrailingSpaces {
			return strings.TrimRight(string(val), " "), nil
		}
		return string(val), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *varStringImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		switch val := v.(type) {
		case nil:
			return types.NullValue, nil
		case bool:
			if val {
				return types.String("1"), nil
			}
			return types.String("0"), nil
		case int:
			return types.String(artifact), nil
		case int8:
			return types.String(artifact), nil
		case int16:
			return types.String(artifact), nil
		case int32:
			return types.String(artifact), nil
		case int64:
			return types.String(artifact), nil
		case uint:
			return types.String(artifact), nil
		case uint8:
			return types.String(artifact), nil
		case uint16:
			return types.String(artifact), nil
		case uint32:
			return types.String(artifact), nil
		case uint64:
			return types.String(artifact), nil
		case float32:
			return types.String(artifact), nil
		case float64:
			return types.String(artifact), nil
		case string:
			if ti.RemoveTrailingSpaces {
				return types.String(strings.TrimRight(val, " ")), nil
			}
			return types.String(val), nil
		case types.Null:
			return types.NullValue, nil
		case types.Bool:
			if val {
				return types.String("1"), nil
			}
			return types.String("0"), nil
		case types.Int:
			return types.String(artifact), nil
		case types.Uint:
			return types.String(artifact), nil
		case types.Float:
			return types.String(artifact), nil
		case types.String:
			if ti.RemoveTrailingSpaces {
				return types.String(strings.TrimRight(string(val), " ")), nil
			}
			return val, nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *varStringImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*varStringImpl); ok {
		return *ti == *ti2
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *varStringImpl) GetTypeIdentifier() Identifier {
	return VarStringTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *varStringImpl) GetTypeParams() map[string]string {
	typeParams := map[string]string{
		varStringTypeParam_Collate: ti.Collation.String(),
		varStringTypeParam_Length:  strconv.FormatInt(ti.MaxLength, 10),
	}
	if ti.RemoveTrailingSpaces {
		typeParams[varStringTypeParam_RemoveTrailingSpaces] = ""
	}
	if ti.IsSqlText {
		typeParams[varStringTypeParam_IsSqlText] = ""
	}
	return typeParams
}

// IsValid implements TypeInfo interface.
func (ti *varStringImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *varStringImpl) NomsKind() types.NomsKind {
	return types.StringKind
}

// String implements TypeInfo interface.
func (ti *varStringImpl) String() string {
	additionalText := ""
	if ti.RemoveTrailingSpaces {
		additionalText = ", RemoveTrailingSpaces"
	}
	if ti.IsSqlText {
		additionalText = ", SqlText"
	}
	return fmt.Sprintf(`VarString(%v, %v%v)`, ti.Collation.String(), ti.MaxLength, additionalText)
}

// ToSqlType implements TypeInfo interface.
func (ti *varStringImpl) ToSqlType() sql.Type {
	// Char is the only type that removes trailing spaces
	if ti.RemoveTrailingSpaces {
		sqlType, err := sql.CreateString(sqltypes.Char, ti.MaxLength, ti.Collation)
		if err == nil {
			return sqlType
		}
	}
	if !ti.IsSqlText {
		sqlType, err := sql.CreateString(sqltypes.VarChar, ti.MaxLength, ti.Collation)
		if err == nil {
			return sqlType
		}
	}
	// The SQL type has a max character limit
	maxLength := ti.MaxLength
	if maxLength > sql.LongText.MaxCharacterLength() {
		maxLength = sql.LongText.MaxCharacterLength()
	}
	sqlType, err := sql.CreateString(sqltypes.Text, maxLength, ti.Collation)
	if err != nil {
		panic(err)
	}
	return sqlType
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *varStringImpl) isValid(v interface{}) (artifact string, ok bool) {
	//TODO: handle collations
	switch val := v.(type) {
	case nil:
		return "", true
	case bool:
		return "", ti.MaxLength >= 1
	case int:
		strVal := strconv.FormatInt(int64(val), 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case int8:
		strVal := strconv.FormatInt(int64(val), 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case int16:
		strVal := strconv.FormatInt(int64(val), 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case int32:
		strVal := strconv.FormatInt(int64(val), 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case int64:
		strVal := strconv.FormatInt(val, 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case uint:
		strVal := strconv.FormatUint(uint64(val), 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case uint8:
		strVal := strconv.FormatUint(uint64(val), 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case uint16:
		strVal := strconv.FormatUint(uint64(val), 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case uint32:
		strVal := strconv.FormatUint(uint64(val), 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case uint64:
		strVal := strconv.FormatUint(val, 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case float32:
		strVal := strconv.FormatFloat(float64(val), 'g', -1, 64)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case float64:
		strVal := strconv.FormatFloat(val, 'g', -1, 64)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case string:
		return "", int64(len(val)) <= ti.MaxLength
	case types.Null:
		return "", true
	case types.Bool:
		return "", ti.MaxLength >= 1
	case types.Int:
		strVal := strconv.FormatInt(int64(val), 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case types.Uint:
		strVal := strconv.FormatUint(uint64(val), 10)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case types.Float:
		strVal := strconv.FormatFloat(float64(val), 'g', -1, 64)
		return strVal, int64(len(strVal)) <= ti.MaxLength
	case types.String:
		return "", int64(len(val)) <= ti.MaxLength
	default:
		return "", false
	}
}
