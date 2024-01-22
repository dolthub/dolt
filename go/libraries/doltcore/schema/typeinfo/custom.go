// Copyright 2024 Dolthub, Inc.
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
	"bytes"
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	customTypeParams_width            = "width"
	customTypeParams_fixed_8          = "8"
	customTypeParams_fixed_16         = "16"
	customTypeParams_fixed_24         = "24"
	customTypeParams_fixed_32         = "32"
	customTypeParams_fixed_64         = "64"
	customTypeParams_fixed_128        = "128"
	customTypeParams_variable_inline  = "inline"
	customTypeParams_variable_chunked = "chunked"
)

//TODO: doc
type customType struct {
	sqlCustomType gmstypes.Custom
}

var _ TypeInfo = (*customType)(nil)

//TODO: doc
func CreateCustomTypeFromParams(params map[string]string) (TypeInfo, error) {
	if width, ok := params[customTypeParams_width]; ok {
		switch width {
		case customTypeParams_fixed_8:
			return nil, fmt.Errorf("target custom type is not yet supported")
		case customTypeParams_fixed_16:
			return nil, fmt.Errorf("target custom type is not yet supported")
		case customTypeParams_fixed_24:
			return nil, fmt.Errorf("target custom type is not yet supported")
		case customTypeParams_fixed_32:
			return nil, fmt.Errorf("target custom type is not yet supported")
		case customTypeParams_fixed_64:
			return nil, fmt.Errorf("target custom type is not yet supported")
		case customTypeParams_fixed_128:
			return nil, fmt.Errorf("target custom type is not yet supported")
		case customTypeParams_variable_inline:
			const testingID = 0
			c, ok := gmstypes.DeserializeCustomType(testingID)
			if !ok {
				return nil, fmt.Errorf("cannot find custom type with id `%d`", testingID)
			}
			return &customType{c}, nil
		case customTypeParams_variable_chunked:
			return nil, fmt.Errorf("target custom type is not yet supported")
		default:
			return nil, fmt.Errorf(`create custom type info has "%v" param with value "%v"`, customTypeParams_width, width)
		}
	}
	return nil, fmt.Errorf(`create custom type info is missing "%v" param`, customTypeParams_width)
}

//TODO: doc
func CreateCustomTypeFromSqlType(typ gmstypes.Custom) *customType {
	return &customType{typ}
}

// ConvertNomsValueToValue implements the TypeInfo interface.
func (ti *customType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.CustomInline); ok {
		return ti.sqlCustomType.DeserializeValue(val[1:])
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *customType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.CustomInlineKind:
		bytes := reader.ReadInlineBlob()
		return ti.sqlCustomType.DeserializeValue(bytes[1:])
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements the TypeInfo interface.
func (ti *customType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	serialized, err := ti.sqlCustomType.SerializeValue(v)
	if err != nil {
		return nil, err
	}
	return types.CustomInline(append([]byte{ti.sqlCustomType.SerializeType()}, serialized...)), nil
}

// Equals implements the TypeInfo interface.
func (ti *customType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*customType); ok {
		return ti.sqlCustomType.Equals(ti2.sqlCustomType)
	}
	return false
}

// FormatValue implements the TypeInfo interface.
func (ti *customType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.CustomInline); ok {
		convVal, err := ti.ConvertNomsValueToValue(val)
		if err != nil {
			return nil, err
		}
		res, err := ti.sqlCustomType.FormatValue(convVal)
		if err != nil {
			return nil, err
		}
		return &res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements the TypeInfo interface.
func (ti *customType) GetTypeIdentifier() Identifier {
	return CustomTypeIdentifier
}

// GetTypeParams implements the TypeInfo interface.
func (ti *customType) GetTypeParams() map[string]string {
	return map[string]string{customTypeParams_width: customTypeParams_variable_inline}
}

// IsValid implements the TypeInfo interface.
func (ti *customType) IsValid(v types.Value) bool {
	if val, ok := v.(types.CustomInline); ok {
		deserialized, err := ti.sqlCustomType.DeserializeValue(val)
		if err != nil {
			return false
		}
		serialized, err := ti.sqlCustomType.SerializeValue(deserialized)
		if err != nil {
			return false
		}
		return bytes.Compare(val, serialized) == 0
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements the TypeInfo interface.
func (ti *customType) NomsKind() types.NomsKind {
	return types.CustomInlineKind
}

// Promote implements the TypeInfo interface.
func (ti *customType) Promote() TypeInfo {
	return &customType{ti.sqlCustomType.Promote().(gmstypes.Custom)}
}

// String implements the TypeInfo interface.
func (ti *customType) String() string {
	return ti.sqlCustomType.String()
}

// ToSqlType implements the TypeInfo interface.
func (ti *customType) ToSqlType() sql.Type {
	return ti.sqlCustomType
}

// customTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func customTypeConverter(ctx context.Context, src *customType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *blobStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *boolType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *customType:
		return wrapIsValid(dest.IsValid, src, dest)
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
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *varStringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *yearType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}
