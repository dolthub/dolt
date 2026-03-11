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

type intType struct {
	sqlIntType sql.NumberType
}

var _ TypeInfo = (*intType)(nil)
var (
	Int8Type  = &intType{gmstypes.Int8}
	Int16Type = &intType{gmstypes.Int16}
	Int24Type = &intType{gmstypes.Int24}
	Int32Type = &intType{gmstypes.Int32}
	Int64Type = &intType{gmstypes.Int64}
)

// Equals implements TypeInfo interface.
func (ti *intType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*intType); ok {
		return ti.sqlIntType.Type() == ti2.sqlIntType.Type() &&
			ti.sqlIntType.DisplayWidth() == ti2.sqlIntType.DisplayWidth()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *intType) NomsKind() types.NomsKind {
	return types.IntKind
}

// String implements TypeInfo interface.
func (ti *intType) String() string {
	switch ti.sqlIntType.Type() {
	case sqltypes.Int8:
		return "Int8"
	case sqltypes.Int16:
		return "Int16"
	case sqltypes.Int24:
		return "Int24"
	case sqltypes.Int32:
		return "Int32"
	case sqltypes.Int64:
		return "Int64"
	default:
		panic(fmt.Errorf(`unknown int type info sql type "%v"`, ti.sqlIntType.Type().String()))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *intType) ToSqlType() sql.Type {
	return ti.sqlIntType
}
