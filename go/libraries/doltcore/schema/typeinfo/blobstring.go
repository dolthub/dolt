// Copyright 2021 Dolthub, Inc.
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
	"unicode/utf8"
	"unsafe"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	blobStringTypeParam_Collate = "collate"
	blobStringTypeParam_Length  = "length"
)

// blobStringType handles the TEXT types. This was originally done in varStringType, however it did not properly
// handle large strings (such as strings over several hundred megabytes), and thus this type was created. Any
// repositories that were made before the introduction of blobStringType will still use varStringType for existing
// columns.
type blobStringType struct {
	sqlStringType sql.StringType
}

var _ TypeInfo = (*blobStringType)(nil)

var (
	TextType     TypeInfo = &blobStringType{sqlStringType: gmstypes.Text}
	LongTextType TypeInfo = &blobStringType{sqlStringType: gmstypes.LongText}
)

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *blobStringType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Blob); ok {
		b, err := fromBlob(val)
		if gmstypes.IsBinaryType(ti.sqlStringType) {
			return b, err
		}
		return string(b), err
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *blobStringType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.PeekKind()
	switch k {
	case types.BlobKind:
		val, err := reader.ReadBlob()
		if err != nil {
			return nil, err
		}
		b, err := fromBlob(val)
		if gmstypes.IsBinaryType(ti.sqlStringType) {
			return b, err
		}
		return string(b), err
	case types.NullKind:
		_ = reader.ReadKind()
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *blobStringType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	strVal, _, err := ti.sqlStringType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}
	val, ok, err := sql.Unwrap[string](ctx, strVal)
	if err != nil {
		return nil, err
	}
	if ok && utf8.ValidString(val) { // We need to move utf8 (collation) validation into the server
		return types.NewBlob(ctx, vrw, strings.NewReader(val))
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *blobStringType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*blobStringType); ok {
		return ti.sqlStringType.MaxCharacterLength() == ti2.sqlStringType.MaxCharacterLength() &&
			ti.sqlStringType.Collation().Equals(ti2.sqlStringType.Collation())
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *blobStringType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.Blob); ok {
		resStr, err := fromBlob(val)
		if err != nil {
			return nil, err
		}
		return (*string)(unsafe.Pointer(&resStr)), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// IsValid implements TypeInfo interface.
func (ti *blobStringType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Blob); ok {
		if int64(val.Len()) <= ti.sqlStringType.MaxByteLength() {
			return true
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *blobStringType) NomsKind() types.NomsKind {
	return types.BlobKind
}

// Promote implements TypeInfo interface.
func (ti *blobStringType) Promote() TypeInfo {
	return &blobStringType{ti.sqlStringType.Promote().(sql.StringType)}
}

// String implements TypeInfo interface.
func (ti *blobStringType) String() string {
	return fmt.Sprintf(`BlobString(%v, %v)`, ti.sqlStringType.Collation().String(), ti.sqlStringType.MaxCharacterLength())
}

// ToSqlType implements TypeInfo interface.
func (ti *blobStringType) ToSqlType() sql.Type {
	return ti.sqlStringType
}
