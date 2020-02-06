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

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Year, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type yearImpl struct{}

var _ TypeInfo = (*yearImpl)(nil)

var YearType TypeInfo = &yearImpl{}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *yearImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Int); ok {
		res, err := sql.Year.Convert(int16(val))
		if err != nil {
			return nil, fmt.Errorf(`"%v" cannot convert year "%v" to value`, ti.String(), val)
		}
		return res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *yearImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		switch v.(type) {
		case nil, types.Null:
			return types.NullValue, nil
		}
		return types.Int(artifact), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *yearImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*yearImpl)
	return ok
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *yearImpl) GetTypeIdentifier() Identifier {
	return YearTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *yearImpl) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *yearImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *yearImpl) NomsKind() types.NomsKind {
	return types.IntKind
}

// String implements TypeInfo interface.
func (ti *yearImpl) String() string {
	return "Year"
}

// ToSqlType implements TypeInfo interface.
func (ti *yearImpl) ToSqlType() sql.Type {
	return sql.Year
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *yearImpl) isValid(v interface{}) (artifact int16, ok bool) {
	// convert some Noms values to their standard golang equivalents, except Null
	switch val := v.(type) {
	case nil:
		return 0, true
	case types.Null:
		return 0, true
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
	res, err := sql.Year.Convert(v)
	resInt, ok := res.(int16)
	return resInt, err == nil && ok
}
