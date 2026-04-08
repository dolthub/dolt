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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// As a type, this is modeled more after MySQL's story for binary data. There, it's treated
// as a string that is interpreted as raw bytes, rather than as a bespoke data structure,
// and thus this is mirrored here in its implementation. This will minimize any differences
// that could arise.
//
// This type handles the BLOB types. BINARY and VARBINARY are handled by inlineBlobType.
type varBinaryType struct {
	sqlBinaryType sql.StringType
	enc           val.Encoding // 0 means use default based on UseAdaptiveEncoding
}

var _ TypeInfo = (*varBinaryType)(nil)

// Equals implements TypeInfo interface.
func (ti *varBinaryType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*varBinaryType); ok {
		return ti.sqlBinaryType.MaxCharacterLength() == ti2.sqlBinaryType.MaxCharacterLength() &&
			ti.Encoding() == ti2.Encoding()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *varBinaryType) NomsKind() types.NomsKind {
	return types.BlobKind
}

// String implements TypeInfo interface.
func (ti *varBinaryType) String() string {
	return fmt.Sprintf(`VarBinary(%v)`, ti.sqlBinaryType.MaxCharacterLength())
}

// Encoding implements TypeInfo interface.
func (ti *varBinaryType) Encoding() val.Encoding {
	if ti.enc != 0 {
		return ti.enc
	}
	if UseAdaptiveEncoding {
		return val.BytesAdaptiveEnc
	}
	return val.BytesAddrEnc
}

// WithEncoding implements TypeInfo interface.
func (ti *varBinaryType) WithEncoding(enc val.Encoding) TypeInfo {
	switch enc {
	case val.BytesAdaptiveEnc, val.BytesAddrEnc:
	default:
		panic(fmt.Errorf("encoding %v is not valid for %T", enc, ti))
	}
	return &varBinaryType{sqlBinaryType: ti.sqlBinaryType, enc: enc}
}

// ToSqlType implements TypeInfo interface.
func (ti *varBinaryType) ToSqlType() sql.Type {
	return ti.sqlBinaryType
}
