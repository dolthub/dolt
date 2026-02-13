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
	"math"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

type boolType struct {
	sqlBitType gmstypes.BitType
}

var _ TypeInfo = (*boolType)(nil)

var BoolType TypeInfo = &boolType{gmstypes.MustCreateBitType(1)}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *boolType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Bool); ok {
		if val {
			return uint64(1), nil
		}
		return uint64(0), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *boolType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.BoolKind:
		b := reader.ReadBool()
		if b {
			return uint64(1), nil
		}

		return uint64(0), nil

	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *boolType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	switch val := v.(type) {
	case nil:
		return types.NullValue, nil
	case bool:
		return types.Bool(val), nil
	case int:
		return types.Bool(val != 0), nil
	case int8:
		return types.Bool(val != 0), nil
	case int16:
		return types.Bool(val != 0), nil
	case int32:
		return types.Bool(val != 0), nil
	case int64:
		return types.Bool(val != 0), nil
	case uint:
		return types.Bool(val != 0), nil
	case uint8:
		return types.Bool(val != 0), nil
	case uint16:
		return types.Bool(val != 0), nil
	case uint32:
		return types.Bool(val != 0), nil
	case uint64:
		return types.Bool(val != 0), nil
	case float32:
		return types.Bool(int64(math.Round(float64(val))) != 0), nil
	case float64:
		return types.Bool(int64(math.Round(val)) != 0), nil
	case string:
		b, err := strconv.ParseBool(val)
		if err == nil {
			return types.Bool(b), nil
		}
		valInt, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf(`"%v" cannot convert value "%v" as it is invalid`, ti.String(), val)
		}
		return types.Bool(valInt != 0), nil
	case []byte:
		return ti.ConvertValueToNomsValue(context.Background(), nil, string(val))
	default:
		return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
	}
}

// Equals implements TypeInfo interface.
func (ti *boolType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*boolType)
	return ok
}

// IsValid implements TypeInfo interface.
func (ti *boolType) IsValid(v types.Value) bool {
	_, err := ti.ConvertNomsValueToValue(v)
	return err == nil
}

// NomsKind implements TypeInfo interface.
func (ti *boolType) NomsKind() types.NomsKind {
	return types.BoolKind
}

// Promote implements TypeInfo interface.
func (ti *boolType) Promote() TypeInfo {
	return ti
}

// String implements TypeInfo interface.
func (ti *boolType) String() string {
	return "Bool"
}

// ToSqlType implements TypeInfo interface.
func (ti *boolType) ToSqlType() sql.Type {
	return gmstypes.Boolean
}
