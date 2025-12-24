// Copyright 2025 Dolthub, Inc.
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
	"strings"
	"unsafe"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/values"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	vectorTypeParam_Length = "length"
)

// As a type, this is modeled more after MySQL's story for binary data. There, it's treated
// as a string that is interpreted as raw bytes, rather than as a bespoke data structure,
// and thus this is mirrored here in its implementation. This will minimize any differences
// that could arise.
//
// This type handles the BLOB types. BINARY and VARBINARY are handled by inlineBlobType.
type vectorType struct {
	sqlVectorType gmstypes.VectorType
}

var _ TypeInfo = (*vectorType)(nil)

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *vectorType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Blob); ok {
		return fromBlob(val)
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti, v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *vectorType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.PeekKind()
	switch k {
	case types.BlobKind:
		val, err := reader.ReadBlob()
		if err != nil {
			return nil, err
		}
		return fromBlob(val)
	case types.NullKind:
		_ = reader.ReadKind()
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *vectorType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	strVal, _, err := ti.sqlVectorType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}
	val, ok := strVal.([]byte)
	if ok {
		return types.NewBlob(ctx, vrw, strings.NewReader(string(val)))
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *vectorType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*vectorType); ok {
		return ti.sqlVectorType.Dimensions == ti2.sqlVectorType.Dimensions
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *vectorType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.Blob); ok {
		resStr, err := fromBlob(val)
		if err != nil {
			return nil, err
		}
		// This is safe (See https://go101.org/article/unsafe.html)
		return (*string)(unsafe.Pointer(&resStr)), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti, v.Kind())
}

// IsValid implements TypeInfo interface.
func (ti *vectorType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Blob); ok {
		if int(val.Len()) == ti.sqlVectorType.Dimensions*int(values.Float32Size) {
			return true
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *vectorType) NomsKind() types.NomsKind {
	return types.BlobKind
}

// Promote implements TypeInfo interface.
func (ti *vectorType) Promote() TypeInfo {
	return ti
}

// String implements TypeInfo interface.
func (ti *vectorType) String() string {
	return fmt.Sprintf(`Vector(%v)`, ti.sqlVectorType.Dimensions)
}

// ToSqlType implements TypeInfo interface.
func (ti *vectorType) ToSqlType() sql.Type {
	return ti.sqlVectorType
}
