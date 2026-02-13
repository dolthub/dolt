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
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

// As a type, this is modeled more after MySQL's story for binary data. There, it's treated
// as a string that is interpreted as raw bytes, rather than as a bespoke data structure,
// and thus this is mirrored here in its implementation. This will minimize any differences
// that could arise.
//
// This type handles the BLOB types. BINARY and VARBINARY are handled by inlineBlobType.
type varBinaryType struct {
	sqlBinaryType sql.StringType
}

var _ TypeInfo = (*varBinaryType)(nil)

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *varBinaryType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Blob); ok {
		return fromBlob(val)
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *varBinaryType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
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
func (ti *varBinaryType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	strVal, _, err := ti.sqlBinaryType.Convert(ctx, v)
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
func (ti *varBinaryType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*varBinaryType); ok {
		return ti.sqlBinaryType.MaxCharacterLength() == ti2.sqlBinaryType.MaxCharacterLength()
	}
	return false
}

// IsValid implements TypeInfo interface.
func (ti *varBinaryType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Blob); ok {
		if int64(val.Len()) <= ti.sqlBinaryType.MaxByteLength() {
			return true
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *varBinaryType) NomsKind() types.NomsKind {
	return types.BlobKind
}

// Promote implements TypeInfo interface.
func (ti *varBinaryType) Promote() TypeInfo {
	return &varBinaryType{ti.sqlBinaryType.Promote().(sql.StringType)}
}

// String implements TypeInfo interface.
func (ti *varBinaryType) String() string {
	return fmt.Sprintf(`VarBinary(%v)`, ti.sqlBinaryType.MaxCharacterLength())
}

// ToSqlType implements TypeInfo interface.
func (ti *varBinaryType) ToSqlType() sql.Type {
	return ti.sqlBinaryType
}

// fromBlob returns a string from a types.Blob.
func fromBlob(b types.Blob) ([]byte, error) {
	strLength := b.Len()
	if strLength == 0 {
		return []byte{}, nil
	}
	str := make([]byte, strLength)
	n, err := b.ReadAt(context.Background(), str, 0)
	if err != nil && err != io.EOF {
		return []byte{}, err
	}
	if uint64(n) != strLength {
		return []byte{}, fmt.Errorf("wanted %d bytes from blob for data, got %d", strLength, n)
	}

	// For very large byte slices, the standard method of converting a byte slice to a string using "string(str)" will
	// cause it to duplicate the entire string. This uses a lot more memory and significantly impact performance.
	// Using an unsafe pointer, we can avoid the duplication and get a fairly large performance gain. In some unofficial
	// testing, performance improved by 40%.
	// This is inspired by Go's own source code in strings.Builder.String(): https://golang.org/src/strings/builder.go#L48
	// This is also marked as a valid strategy in unsafe.Pointer's own method documentation.
	return str, nil
}
