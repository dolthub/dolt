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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Time, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type timeType struct {
	sqlTimeType gmstypes.TimeType
}

var _ TypeInfo = (*timeType)(nil)

var TimeType = &timeType{gmstypes.Time}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *timeType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	// TODO: Add context parameter to ConvertNomsValueToValue, or delete the typeinfo package
	ctx := context.Background()
	if val, ok := v.(types.Int); ok {
		ret, _, err := ti.sqlTimeType.Convert(ctx, gmstypes.Timespan(val))
		return ret, err
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *timeType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	// TODO: Add context parameter to ReadFrom, or delete the typeinfo package
	ctx := context.Background()
	k := reader.ReadKind()
	switch k {
	case types.IntKind:
		val := reader.ReadInt()
		ret, _, err := ti.sqlTimeType.Convert(ctx, gmstypes.Timespan(val))
		return ret, err
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *timeType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	val, _, err := ti.sqlTimeType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}
	return types.Int(val.(gmstypes.Timespan)), nil
}

// Equals implements TypeInfo interface.
func (ti *timeType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*timeType)
	return ok
}

// IsValid implements TypeInfo interface.
func (ti *timeType) IsValid(v types.Value) bool {
	if _, ok := v.(types.Int); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *timeType) NomsKind() types.NomsKind {
	return types.IntKind
}

// Promote implements TypeInfo interface.
func (ti *timeType) Promote() TypeInfo {
	return &timeType{ti.sqlTimeType.Promote().(gmstypes.TimeType)}
}

// String implements TypeInfo interface.
func (ti *timeType) String() string {
	return "Time"
}

// ToSqlType implements TypeInfo interface.
func (ti *timeType) ToSqlType() sql.Type {
	return ti.sqlTimeType
}
