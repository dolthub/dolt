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
	"fmt"
	"math"
	"unsafe"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

type inlineBlobType struct {
	sqlBinaryType sql.StringType
}

var _ TypeInfo = (*inlineBlobType)(nil)

var InlineBlobType = &inlineBlobType{sql.MustCreateBinary(sqltypes.VarBinary, math.MaxUint16)}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *inlineBlobType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.InlineBlob); ok {
		return *(*string)(unsafe.Pointer(&val)), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *inlineBlobType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.InlineBlobKind:
		bytes := reader.ReadInlineBlob()
		return *(*string)(unsafe.Pointer(&bytes)), nil
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *inlineBlobType) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlBinaryType.Convert(v)
	if err != nil {
		return nil, err
	}
	val, ok := strVal.(string)
	if ok {
		return *(*types.InlineBlob)(unsafe.Pointer(&val)), nil
	}
	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
}

// Equals implements TypeInfo interface.
func (ti *inlineBlobType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*inlineBlobType)
	return ok
}

// FormatValue implements TypeInfo interface.
func (ti *inlineBlobType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.InlineBlob); ok {
		convVal, err := ti.ConvertNomsValueToValue(val)
		if err != nil {
			return nil, err
		}
		res, ok := convVal.(string)
		if !ok {
			return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
		}
		return &res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *inlineBlobType) GetTypeIdentifier() Identifier {
	return InlineBlobTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *inlineBlobType) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *inlineBlobType) IsValid(v types.Value) bool {
	if _, ok := v.(types.InlineBlob); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *inlineBlobType) NomsKind() types.NomsKind {
	return types.InlineBlobKind
}

// ParseValue implements TypeInfo interface.
func (ti *inlineBlobType) ParseValue(str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlBinaryType.Convert(*str)
	if err != nil {
		return nil, err
	}
	if val, ok := strVal.(string); ok {
		return *(*types.InlineBlob)(unsafe.Pointer(&val)), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert the string "%v" to a value`, ti.String(), str)
}

// Promote implements TypeInfo interface.
func (ti *inlineBlobType) Promote() TypeInfo {
	return &inlineBlobType{ti.sqlBinaryType.Promote().(sql.StringType)}
}

// String implements TypeInfo interface.
func (ti *inlineBlobType) String() string {
	return "InlineBlob"
}

// ToSqlType implements TypeInfo interface.
func (ti *inlineBlobType) ToSqlType() sql.Type {
	return ti.sqlBinaryType
}
