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

"github.com/liquidata-inc/dolt/go/store/types"
"github.com/src-d/go-mysql-server/sql"
)

// This is a dolt implementation of the MySQL type Time, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type timeImpl struct{}

var _ TypeInfo = (*timeImpl)(nil)

func CreateTimeType(map[string]string) (TypeInfo, error) {
	return &timeImpl{}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *timeImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.String); ok {
		res, err := sql.Time.Convert(string(val))
		if err != nil {
			return nil, fmt.Errorf(`"%v" cannot convert "%v" to value`, ti.String(), val)
		}
		return res, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *timeImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		return types.String(artifact), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *timeImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*timeImpl)
	return ok
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *timeImpl) GetTypeIdentifier() Identifier {
	return TimeType
}

// GetTypeParams implements TypeInfo interface.
func (ti *timeImpl) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *timeImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *timeImpl) NomsKind() types.NomsKind {
	return types.StringKind
}

// String implements TypeInfo interface.
func (ti *timeImpl) String() string {
	return "Time"
}

// ToSqlType implements TypeInfo interface.
func (ti *timeImpl) ToSqlType() sql.Type {
	return sql.Time
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *timeImpl) isValid(v interface{}) (artifact string, ok bool) {
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
	res, err := sql.Time.Convert(v)
	resStr, ok := res.(string)
	return resStr, err == nil && ok
}
