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

// This is a dolt implementation of the MySQL type Time, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type timeType struct {
	sqlTimeType sql.TimeType
}

var _ TypeInfo = (*timeType)(nil)

var TimeType = &timeType{sql.Time}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *timeType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	//TODO: expose the MySQL type's microsecond implementation and persist that to disk? Enables sorting
	if val, ok := v.(types.String); ok {
		return string(val), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *timeType) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlTimeType.Convert(v)
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
func (ti *timeType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*timeType)
	return ok
}

// FormatValue implements TypeInfo interface.
func (ti *timeType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.String); ok {
		res := string(val)
		return &res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *timeType) GetTypeIdentifier() Identifier {
	return TimeTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *timeType) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *timeType) IsValid(v types.Value) bool {
	_, err := ti.ConvertNomsValueToValue(v)
	return err == nil
}

// NomsKind implements TypeInfo interface.
func (ti *timeType) NomsKind() types.NomsKind {
	return types.StringKind
}

// ParseValue implements TypeInfo interface.
func (ti *timeType) ParseValue(str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlTimeType.Convert(*str)
	if err != nil {
		return nil, err
	}
	if val, ok := strVal.(string); ok {
		return types.String(val), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert the string "%v" to a value`, ti.String(), str)
}

// String implements TypeInfo interface.
func (ti *timeType) String() string {
	return "Time"
}

// ToSqlType implements TypeInfo interface.
func (ti *timeType) ToSqlType() sql.Type {
	return ti.sqlTimeType
}
