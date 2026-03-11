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
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

type uuidType struct {
	sqlCharType sql.StringType
}

var _ TypeInfo = (*uuidType)(nil)

var UuidType = &uuidType{gmstypes.MustCreateString(sqltypes.Char, 36, sql.Collation_ascii_bin)}

// Equals implements TypeInfo interface.
func (ti *uuidType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*uuidType)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *uuidType) NomsKind() types.NomsKind {
	return types.UUIDKind
}

// String implements TypeInfo interface.
func (ti *uuidType) String() string {
	return "Uuid"
}

// ToSqlType implements TypeInfo interface.
func (ti *uuidType) ToSqlType() sql.Type {
	return ti.sqlCharType
}
