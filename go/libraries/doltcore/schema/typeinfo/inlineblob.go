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
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	"vitess.io/vitess/go/sqltypes"

	"github.com/liquidata-inc/dolt/go/store/types"
)

type inlineBlobImpl struct{}

var _ TypeInfo = (*inlineBlobImpl)(nil)

var InlineBlobType TypeInfo = &inlineBlobImpl{}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *inlineBlobImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.InlineBlob); ok {
		return strings.ToUpper(hex.EncodeToString(val)), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *inlineBlobImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if _, ok := ti.isValid(v); ok {
		switch val := v.(type) {
		case nil:
			return types.NullValue, nil
		case []byte:
			return types.InlineBlob(val), nil
		case string:
			return types.InlineBlob(val), nil
		case types.Null:
			return types.NullValue, nil
		case types.InlineBlob:
			return val, nil
		case types.String:
			return types.InlineBlob(val), nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *inlineBlobImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*inlineBlobImpl)
	return ok
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *inlineBlobImpl) GetTypeIdentifier() Identifier {
	return InlineBlobTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *inlineBlobImpl) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *inlineBlobImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *inlineBlobImpl) NomsKind() types.NomsKind {
	return types.InlineBlobKind
}

// String implements TypeInfo interface.
func (ti *inlineBlobImpl) String() string {
	return "InlineBlob"
}

// ToSqlType implements TypeInfo interface.
func (ti *inlineBlobImpl) ToSqlType() sql.Type {
	return sql.MustCreateBinary(sqltypes.VarBinary, math.MaxUint16)
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *inlineBlobImpl) isValid(v interface{}) (artifact []byte, ok bool) {
	switch val := v.(type) {
	case nil:
		return nil, true
	case []byte:
		return nil, len(val) <= math.MaxUint16
	case string:
		return nil, len(val) <= math.MaxUint16
	case types.Null:
		return nil, true
	case types.InlineBlob:
		return nil, len(val) <= math.MaxUint16
	case types.String:
		return nil, len(val) <= math.MaxUint16
	default:
		return nil, false
	}
}
