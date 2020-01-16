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
	"strconv"
	"vitess.io/vitess/go/sqltypes"

	"github.com/liquidata-inc/dolt/go/store/types"

	"github.com/src-d/go-mysql-server/sql"
)

const (
	varBinaryTypeParam_Length = "length"
	varBinaryTypeParam_PadBytes = "pad"
)

// As a type, this is modeled more after MySQL's story for binary data. There, it's treated
// as a string that is interpreted as raw bytes, rather than as a bespoke data structure,
// and thus this is mirrored here in its implementation. This will minimize any differences
// that could arise.
type varBinaryImpl struct{
	MaxLength int64
	PadBytes bool
}

var _ TypeInfo = (*varBinaryImpl)(nil)

func CreateVarBinaryType(params map[string]string) (TypeInfo, error) {
	ti := &varBinaryImpl{}
	var err error
	if lengthStr, ok := params[varBinaryTypeParam_Length]; ok {
		ti.MaxLength, err = strconv.ParseInt(lengthStr, 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create varbinary type info is missing param "%v"`, varBinaryTypeParam_Length)
	}
	if _, ok := params[varBinaryTypeParam_PadBytes]; ok {
		ti.PadBytes = true
	}
	return ti, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *varBinaryImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.String); ok {
		if ti.PadBytes {
			return ti.padBytes(string(val)), nil
		}
		return string(val), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *varBinaryImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		switch val := v.(type) {
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
			if ti.PadBytes {
				return types.String(ti.padBytes(val)), nil
			}
			return types.String(val), nil
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
			if ti.PadBytes{
				return types.String(ti.padBytes(string(val))), nil
			}
			return val, nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *varBinaryImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*varBinaryImpl); ok {
		return ti.MaxLength == ti2.MaxLength && ti.PadBytes == ti2.PadBytes
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *varBinaryImpl) GetTypeIdentifier() Identifier {
	return VarBinaryType
}

// GetTypeParams implements TypeInfo interface.
func (ti *varBinaryImpl) GetTypeParams() map[string]string {
	typeParams := map[string]string{
		varBinaryTypeParam_Length:  strconv.FormatInt(ti.MaxLength, 10),
	}
	if ti.PadBytes {
		typeParams[varBinaryTypeParam_PadBytes] = ""
	}
	return typeParams
}

// IsValid implements TypeInfo interface.
func (ti *varBinaryImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *varBinaryImpl) NomsKind() types.NomsKind {
	return types.StringKind
}

// String implements TypeInfo interface.
func (ti *varBinaryImpl) String() string {
	padBytes := ""
	if ti.PadBytes {
		padBytes = ", PadBytes"
	}
	return fmt.Sprintf(`VarBinary(%v%v)`, ti.MaxLength, padBytes)
}

// ToSqlType implements TypeInfo interface.
func (ti *varBinaryImpl) ToSqlType() sql.Type {
	// Binary is the only type that pads bytes.
	if ti.PadBytes {
		sqlType, err := sql.CreateBlob(sqltypes.Binary, ti.MaxLength)
		if err == nil {
			return sqlType
		}
	}
	// VarBinary is more restrictive than Blob
	sqlType, err := sql.CreateBlob(sqltypes.VarBinary, ti.MaxLength)
	if err != nil {
		sqlType, err = sql.CreateBlob(sqltypes.Blob, ti.MaxLength)
		if err != nil {
			panic(err)
		}
	}
	return sqlType
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *varBinaryImpl) isValid(v interface{}) (artifact string, ok bool) {
	switch val := v.(type) {
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

// padBytes pads a string with zero bytes if the string length is less than the max length.
func (ti *varBinaryImpl) padBytes(v string) string {
	if int64(len(v)) < ti.MaxLength {
		return string(append([]byte(v), make([]byte, ti.MaxLength - int64(len(v)))...))
	}
	return v
}
