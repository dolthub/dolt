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

type FloatWidth int8

type floatType struct {
	sqlFloatType sql.NumberType
}

var _ TypeInfo = (*floatType)(nil)
var (
	Float32Type = &floatType{gmstypes.Float32}
	Float64Type = &floatType{gmstypes.Float64}
)

// Equals implements TypeInfo interface.
func (ti *floatType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*floatType); ok {
		return ti.sqlFloatType.Type() == ti2.sqlFloatType.Type()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *floatType) NomsKind() types.NomsKind {
	return types.FloatKind
}

// String implements TypeInfo interface.
func (ti *floatType) String() string {
	switch ti.sqlFloatType.Type() {
	case sqltypes.Float32:
		return "Float32"
	case sqltypes.Float64:
		return "Float64"
	default:
		panic(fmt.Errorf(`unknown float type info sql type "%v"`, ti.sqlFloatType.Type().String()))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *floatType) ToSqlType() sql.Type {
	return ti.sqlFloatType
}
