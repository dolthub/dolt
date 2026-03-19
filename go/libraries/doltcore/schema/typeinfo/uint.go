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
	"github.com/dolthub/dolt/go/store/val"
)

type uintType struct {
	sqlUintType sql.NumberType
	enc         val.Encoding
}

var _ TypeInfo = (*uintType)(nil)
var (
	Uint8Type  = &uintType{sqlUintType: gmstypes.Uint8}
	Uint16Type = &uintType{sqlUintType: gmstypes.Uint16}
	Uint24Type = &uintType{sqlUintType: gmstypes.Uint24}
	Uint32Type = &uintType{sqlUintType: gmstypes.Uint32}
	Uint64Type = &uintType{sqlUintType: gmstypes.Uint64}
)

// Equals implements TypeInfo interface.
func (ti *uintType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*uintType); ok {
		return ti.sqlUintType.Type() == ti2.sqlUintType.Type() &&
			ti.sqlUintType.DisplayWidth() == ti2.sqlUintType.DisplayWidth() &&
			ti.Encoding() == ti2.Encoding()
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

// Encoding implements TypeInfo interface.
func (ti *uintType) Encoding() val.Encoding {
	if ti.enc != 0 {
		return ti.enc
	}
	switch ti.sqlUintType.Type() {
	case sqltypes.Uint8:
		return val.Uint8Enc
	case sqltypes.Uint16:
		return val.Uint16Enc
	case sqltypes.Uint24, sqltypes.Uint32:
		return val.Uint32Enc
	case sqltypes.Uint64:
		return val.Uint64Enc
	default:
		panic(fmt.Errorf(`unknown uint type info sql type "%v"`, ti.sqlUintType.Type().String()))
	}
}

// WithEncoding implements TypeInfo interface.
func (ti *uintType) WithEncoding(enc val.Encoding) TypeInfo {
	if enc != (&uintType{sqlUintType: ti.sqlUintType}).Encoding() {
		panic(fmt.Errorf("encoding %v is not valid for %T", enc, ti))
	}
	return &uintType{sqlUintType: ti.sqlUintType, enc: enc}
}

// ToSqlType implements TypeInfo interface.
func (ti *uintType) ToSqlType() sql.Type {
	return ti.sqlUintType
}
