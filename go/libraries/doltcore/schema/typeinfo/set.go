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
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	setTypeParam_Collation = "collate"
	setTypeParam_Values    = "vals"
)

// This is a dolt implementation of the MySQL type Set, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type setType struct {
	sqlSetType sql.SetType
}

var _ TypeInfo = (*setType)(nil)

func CreateSetTypeFromParams(params map[string]string) (TypeInfo, error) {
	collationStr, ok := params[setTypeParam_Collation]
	if !ok {
		return nil, fmt.Errorf(`create set type info is missing param "%v"`, setTypeParam_Collation)
	}
	collation, err := sql.ParseCollation("", collationStr, false)
	if err != nil {
		return nil, err
	}

	valuesStr, ok := params[setTypeParam_Values]
	if !ok {
		return nil, fmt.Errorf(`create set type info is missing param "%v"`, setTypeParam_Values)
	}
	var values []string
	dec := gob.NewDecoder(strings.NewReader(valuesStr))
	if err = dec.Decode(&values); err != nil {
		return nil, err
	}

	sqlSetType, err := gmstypes.CreateSetType(values, collation)
	if err != nil {
		return nil, err
	}
	return CreateSetTypeFromSqlSetType(sqlSetType), nil
}

func CreateSetTypeFromSqlSetType(sqlSetType sql.SetType) TypeInfo {
	return &setType{sqlSetType}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *setType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Uint); ok {
		return uint64(val), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *setType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.UintKind:
		return reader.ReadUint(), nil
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *setType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	val, _, err := ti.sqlSetType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}
	return types.Uint(val.(uint64)), nil
}

// Equals implements TypeInfo interface.
func (ti *setType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*setType); ok && ti.sqlSetType.NumberOfElements() == ti2.sqlSetType.NumberOfElements() {
		tiVals := ti.sqlSetType.Values()
		ti2Vals := ti2.sqlSetType.Values()
		for i := range tiVals {
			if tiVals[i] != ti2Vals[i] {
				return false
			}
		}
		return true
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *setType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	convVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	val, err := ti.sqlSetType.BitsToString(convVal.(uint64))
	if err != nil {
		return nil, err
	}
	return &val, nil
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *setType) GetTypeIdentifier() Identifier {
	return SetTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *setType) GetTypeParams() map[string]string {
	var sb strings.Builder
	enc := gob.NewEncoder(&sb)
	err := enc.Encode(ti.sqlSetType.Values())
	// this should never error, encoding an array of strings should always succeed
	if err != nil {
		panic(err)
	}
	return map[string]string{
		setTypeParam_Collation: ti.sqlSetType.Collation().String(),
		setTypeParam_Values:    sb.String(),
	}
}

// IsValid implements TypeInfo interface.
func (ti *setType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Uint); ok {
		_, err := ti.sqlSetType.BitsToString(uint64(val))
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
func (ti *setType) NomsKind() types.NomsKind {
	return types.UintKind
}

// Promote implements TypeInfo interface.
func (ti *setType) Promote() TypeInfo {
	return &setType{ti.sqlSetType.Promote().(sql.SetType)}
}

// String implements TypeInfo interface.
func (ti *setType) String() string {
	return fmt.Sprintf(`Set(Collation: %v, Values: %v)`, ti.sqlSetType.Collation().String(), strings.Join(ti.sqlSetType.Values(), ","))
}

// ToSqlType implements TypeInfo interface.
func (ti *setType) ToSqlType() sql.Type {
	return ti.sqlSetType
}

// setTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func setTypeConverter(ctx context.Context, src *setType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
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
		return wrapIsValid(dest.IsValid, src, dest)
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
