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
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

// inlineBlobType handles BINARY and VARBINARY. BLOB types are handled by varBinaryType.
type inlineBlobType struct {
	sqlBinaryType sql.StringType
}

var _ TypeInfo = (*inlineBlobType)(nil)

// Equals implements TypeInfo interface.
func (ti *inlineBlobType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*inlineBlobType); ok {
		return ti.sqlBinaryType.MaxCharacterLength() == ti2.sqlBinaryType.MaxCharacterLength() &&
			ti.sqlBinaryType.Type() == ti2.sqlBinaryType.Type()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *inlineBlobType) NomsKind() types.NomsKind {
	return types.InlineBlobKind
}

// String implements TypeInfo interface.
func (ti *inlineBlobType) String() string {
	sqlType := ""
	switch ti.sqlBinaryType.Type() {
	case sqltypes.Binary:
		sqlType = "Binary"
	case sqltypes.VarBinary:
		sqlType = "VarBinary"
	default:
		panic(fmt.Errorf(`unknown inlineblob type info sql type "%v"`, ti.sqlBinaryType.Type().String()))
	}
	return fmt.Sprintf(`InlineBlob(%v, SQL: %v)`, ti.sqlBinaryType.MaxCharacterLength(), sqlType)
}

// ToSqlType implements TypeInfo interface.
func (ti *inlineBlobType) ToSqlType() sql.Type {
	return ti.sqlBinaryType
}
