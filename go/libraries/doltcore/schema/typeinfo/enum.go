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
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	enumTypeParam_Collation = "collate"
	enumTypeParam_Values    = "vals"
)

// This is a dolt implementation of the MySQL type Enum, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type enumImpl struct {
	sqlEnumType sql.EnumType
}

var _ TypeInfo = (*enumImpl)(nil)

func CreateEnumTypeFromParams(params map[string]string) (TypeInfo, error) {
	var collation sql.Collation
	var err error
	if collationStr, ok := params[enumTypeParam_Collation]; ok {
		collation, err = sql.ParseCollation(nil, &collationStr, false)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create enum type info is missing param "%v"`, enumTypeParam_Collation)
	}
	var values []string
	if valuesStr, ok := params[enumTypeParam_Values]; ok {
		dec := gob.NewDecoder(strings.NewReader(valuesStr))
		err = dec.Decode(&values)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create enum type info is missing param "%v"`, enumTypeParam_Values)
	}
	sqlEnumType, err := sql.CreateEnumType(values, collation)
	if err != nil {
		return nil, err
	}
	return &enumImpl{sqlEnumType}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *enumImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.String); ok {
		res, err := ti.sqlEnumType.Convert(string(val))
		if err != nil {
			return nil, fmt.Errorf(`"%v" cannot convert "%v" to value`, ti.String(), val)
		}
		return res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *enumImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		switch v.(type) {
		case nil, types.Null:
			return types.NullValue, nil
		}
		return types.String(artifact), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *enumImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*enumImpl); ok && ti.sqlEnumType.NumberOfElements() == ti2.sqlEnumType.NumberOfElements() {
		tiVals := ti.sqlEnumType.Values()
		ti2Vals := ti2.sqlEnumType.Values()
		for i := range tiVals {
			if tiVals[i] != ti2Vals[i] {
				return false
			}
		}
		return true
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *enumImpl) GetTypeIdentifier() Identifier {
	return EnumTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *enumImpl) GetTypeParams() map[string]string {
	var sb strings.Builder
	enc := gob.NewEncoder(&sb)
	err := enc.Encode(ti.sqlEnumType.Values())
	// this should never error, encoding an array of strings should always succeed
	if err != nil {
		panic(err)
	}
	return map[string]string{
		enumTypeParam_Collation: ti.sqlEnumType.Collation().String(),
		enumTypeParam_Values:    sb.String(),
	}
}

// IsValid implements TypeInfo interface.
func (ti *enumImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *enumImpl) NomsKind() types.NomsKind {
	return types.StringKind
}

// String implements TypeInfo interface.
func (ti *enumImpl) String() string {
	return fmt.Sprintf(`Enum(Collation: %v, Values: %v)`, ti.sqlEnumType.Collation().String(), strings.Join(ti.sqlEnumType.Values(), ", "))
}

// ToSqlType implements TypeInfo interface.
func (ti *enumImpl) ToSqlType() sql.Type {
	return ti.sqlEnumType
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *enumImpl) isValid(v interface{}) (artifact string, ok bool) {
	// convert some Noms values to their standard golang equivalents, except Null
	switch val := v.(type) {
	case nil:
		return "", true
	case types.Null:
		return "", true
	case types.Bool:
		v = bool(val)
	case types.Int:
		v = int64(val)
	case types.Uint:
		v = uint64(val)
	case types.Float:
		v = float64(val)
	case types.String:
		v = string(val)
	}
	res, err := ti.sqlEnumType.Convert(v)
	resStr, ok := res.(string)
	return resStr, err == nil && ok
}
