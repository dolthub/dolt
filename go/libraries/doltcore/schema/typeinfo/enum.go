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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	enumTypeParam_Collation = "collate"
	enumTypeParam_Values    = "vals"
)

// This is a dolt implementation of the MySQL type Enum, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type enumType struct {
	sqlEnumType sql.EnumType
}

var _ TypeInfo = (*enumType)(nil)

func CreateEnumTypeFromSqlEnumType(sqlEnumType sql.EnumType) TypeInfo {
	return &enumType{sqlEnumType}
}

// Equals implements TypeInfo interface.
func (ti *enumType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*enumType); ok && ti.sqlEnumType.NumberOfElements() == ti2.sqlEnumType.NumberOfElements() {
		tiVals := ti.sqlEnumType.Values()
		ti2Vals := ti2.sqlEnumType.Values()
		for i := range tiVals {
			if tiVals[i] != ti2Vals[i] {
				return false
			}
		}
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *enumType) NomsKind() types.NomsKind {
	return types.UintKind
}

// String implements TypeInfo interface.
func (ti *enumType) String() string {
	return fmt.Sprintf(`Enum(Collation: %v, Values: %v)`, ti.sqlEnumType.Collation().String(), strings.Join(ti.sqlEnumType.Values(), ", "))
}

// ToSqlType implements TypeInfo interface.
func (ti *enumType) ToSqlType() sql.Type {
	return ti.sqlEnumType
}
