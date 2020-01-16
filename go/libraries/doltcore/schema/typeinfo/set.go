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
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/src-d/go-mysql-server/sql"
)

const (
	setTypeParam_Collation = "collate"
	setTypeParam_Values = "vals"
)

// This is a dolt implementation of the MySQL type Set, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type setImpl struct{
	sqlSetType sql.SetType
}

var _ TypeInfo = (*setImpl)(nil)

func CreateSetType(params map[string]string) (TypeInfo, error) {
	var collation sql.Collation
	var err error
	if collationStr, ok := params[setTypeParam_Collation]; ok {
		collation, err = sql.ParseCollation(nil, &collationStr, false)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create set type info is missing param "%v"`, setTypeParam_Collation)
	}
	var values []string
	if valuesStr, ok := params[setTypeParam_Values]; ok {
		dec := gob.NewDecoder(strings.NewReader(valuesStr))
		err = dec.Decode(&values)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create set type info is missing param "%v"`, setTypeParam_Values)
	}
	sqlSetType, err := sql.CreateSetType(values, collation)
	if err != nil {
		return nil, err
	}
	return &setImpl{sqlSetType}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *setImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.String); ok {
		res, err := ti.sqlSetType.Convert(string(val))
		if err != nil {
			return nil, fmt.Errorf(`"%v" cannot convert "%v" to value`, ti.String(), val)
		}
		return res, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *setImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		return types.String(artifact), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *setImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*setImpl); ok && ti.sqlSetType.NumberOfElements() == ti2.sqlSetType.NumberOfElements() {
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

// GetTypeIdentifier implements TypeInfo interface.
func (ti *setImpl) GetTypeIdentifier() Identifier {
	return SetType
}

// GetTypeParams implements TypeInfo interface.
func (ti *setImpl) GetTypeParams() map[string]string {
	var sb strings.Builder
	enc := gob.NewEncoder(&sb)
	err := enc.Encode(ti.sqlSetType.Values())
	// this should never error, encoding an array of strings should always succeed
	if err != nil {
		panic(err)
	}
	return map[string]string{
		setTypeParam_Collation: ti.sqlSetType.Collation().String(),
		setTypeParam_Values: sb.String(),
	}
}

// IsValid implements TypeInfo interface.
func (ti *setImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *setImpl) NomsKind() types.NomsKind {
	return types.StringKind
}

// String implements TypeInfo interface.
func (ti *setImpl) String() string {
	return fmt.Sprintf(`Set(Collation: "%v", Values: %v)`, ti.sqlSetType.Collation().String(), strings.Join(ti.sqlSetType.Values(), ","))
}

// ToSqlType implements TypeInfo interface.
func (ti *setImpl) ToSqlType() sql.Type {
	return ti.sqlSetType
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *setImpl) isValid(v interface{}) (artifact string, ok bool) {
	// convert some Noms values to their standard golang equivalents
	switch val := v.(type) {
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
	res, err := ti.sqlSetType.Convert(v)
	resStr, ok := res.(string)
	return resStr, err == nil && ok
}
