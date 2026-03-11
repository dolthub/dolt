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

type uintType struct {
	sqlUintType sql.NumberType
}

var _ TypeInfo = (*uintType)(nil)
var (
	Uint8Type  = &uintType{gmstypes.Uint8}
	Uint16Type = &uintType{gmstypes.Uint16}
	Uint24Type = &uintType{gmstypes.Uint24}
	Uint32Type = &uintType{gmstypes.Uint32}
	Uint64Type = &uintType{gmstypes.Uint64}
)

// Equals implements TypeInfo interface.
func (ti *uintType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*uintType); ok {
		return ti.sqlUintType.Type() == ti2.sqlUintType.Type() &&
			ti.sqlUintType.DisplayWidth() == ti2.sqlUintType.DisplayWidth()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *uintType) NomsKind() types.NomsKind {
	return types.UintKind
}

// String implements TypeInfo interface.
func (ti *uintType) String() string {
	switch ti.sqlUintType.Type() {
	case sqltypes.Uint8:
		return "Uint8"
	case sqltypes.Uint16:
		return "Uint16"
	case sqltypes.Uint24:
		return "Uint24"
	case sqltypes.Uint32:
		return "Uint32"
	case sqltypes.Uint64:
		return "Uint64"
	default:
		panic(fmt.Errorf(`unknown uint type info sql type "%v"`, ti.sqlUintType.Type().String()))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *uintType) ToSqlType() sql.Type {
	return ti.sqlUintType
}
