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

/*// This is a dolt implementation of the MySQL type Geometry, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type geometryType struct {
	geometryType sql.GeometryType
}

var _ TypeInfo = (*geometryType)(nil)
var GeometryType = &geometryType{sql.Geometry}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *geometryType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Geometry); ok {
		return geometry.NomsGeometry(val), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *geometryType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.GeometryKind:
		gs, err := reader.ReadGeometry()
		if err != nil {
			return nil, err
		}
		return geometry.NomsGeometry(gs), nil
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *geometryType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}

	gsObj, err := ti.geometryType.Convert(v)
	if err != nil {
		return nil, err
	}

	gsVal, ok := gsObj.(sql.GeometryValue)
	if !ok {
		return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
	}

	noms, err := geometry.NomsGeometryFromGeometryValue(ctx, vrw, gsVal)
	if err != nil {
		return nil, err
	}

	return types.Geometry(noms), err
}

// Equals implements TypeInfo interface.
func (ti *geometryType) Equals(other TypeInfo) bool {
	return ti.GetTypeIdentifier() == other.GetTypeIdentifier()
}

// FormatValue implements TypeInfo interface.
func (ti *geometryType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}

	if noms, ok := v.(types.Geometry); ok {
		s, err := geometry.NomsGeometry(noms).ToString(sql.NewEmptyContext())
		if err != nil {
			return nil, err
		}
		return &s, nil
	}
	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *geometryType) GetTypeIdentifier() Identifier {
	return GeometryTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *geometryType) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *geometryType) IsValid(v types.Value) bool {
	if v == nil {
		return true
	}
	switch v.(type) {
	case types.Geometry:
		return true
	case types.Null:
		return true
	default:
		return false
	}
}

// NomsKind implements TypeInfo interface.
func (ti *geometryType) NomsKind() types.NomsKind {
	return types.GeometryKind
}

// ParseValue implements TypeInfo interface.
func (ti *geometryType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil {
		return types.NullValue, nil
	}
	return ti.ConvertValueToNomsValue(ctx, vrw, *str)
}

// Promote implements TypeInfo interface.
func (ti *geometryType) Promote() TypeInfo {
	return &geometryType{ti.geometryType.Promote().(sql.GeometryType)}
}

// String implements TypeInfo interface.
func (ti *geometryType) String() string {
	return "Geometry"
}

// ToSqlType implements TypeInfo interface.
func (ti *geometryType) ToSqlType() sql.Type {
	return ti.geometryType
}

// geometryTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func geometryTypeConverter(ctx context.Context, src *geometryType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
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
	case *geometryType:
		return wrapIsValid(dest.IsValid, src, dest)
	case *jsonType:
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
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}
*/
