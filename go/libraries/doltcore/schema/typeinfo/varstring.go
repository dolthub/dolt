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
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

// varStringType handles CHAR and VARCHAR. The TEXT types are handled by blobStringType. For any repositories that were
// created before the introduction of blobStringType, they will use varStringType for TEXT types. As varStringType makes
// use of the String Value type, it does not actually support all viable lengths of a TEXT string, meaning all such
// legacy repositories will run into issues if they attempt to insert very large strings. Any and all new repositories
// must use blobStringType for all TEXT types to ensure proper behavior.
type varStringType struct {
	sqlStringType sql.StringType
}

var _ TypeInfo = (*varStringType)(nil)

var (
	MaxVarcharLength = int64(16383)
	// StringDefaultType is sized to 1k, which allows up to 16 fields per row
	StringDefaultType = &varStringType{gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, MaxVarcharLength/16)}
	// StringImportDefaultType is sized to 200, which allows up to 80+ fields per row during import operations
	StringImportDefaultType = &varStringType{gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 200)}
)

func CreateVarStringTypeFromSqlType(stringType sql.StringType) TypeInfo {
	return &varStringType{stringType}
}

// Equals implements TypeInfo interface.
func (ti *varStringType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*varStringType); ok {
		return ti.sqlStringType.MaxCharacterLength() == ti2.sqlStringType.MaxCharacterLength() &&
			ti.sqlStringType.Type() == ti2.sqlStringType.Type() &&
			ti.sqlStringType.Collation().Equals(ti2.sqlStringType.Collation())
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *varStringType) NomsKind() types.NomsKind {
	return types.StringKind
}

// String implements TypeInfo interface.
func (ti *varStringType) String() string {
	sqlType := ""
	switch ti.sqlStringType.Type() {
	case sqltypes.Char:
		sqlType = "Char"
	case sqltypes.VarChar:
		sqlType = "VarChar"
	case sqltypes.Text:
		sqlType = "Text"
	default:
		panic(fmt.Errorf(`unknown varstring type info sql type "%v"`, ti.sqlStringType.Type().String()))
	}
	return fmt.Sprintf(`VarString(%v, %v, SQL: %v)`, ti.sqlStringType.Collation().String(), ti.sqlStringType.MaxCharacterLength(), sqlType)
}

// ToSqlType implements TypeInfo interface.
func (ti *varStringType) ToSqlType() sql.Type {
	return ti.sqlStringType
}
