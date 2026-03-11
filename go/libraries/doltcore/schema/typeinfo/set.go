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

// This is a dolt implementation of the MySQL type Set, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type setType struct {
	sqlSetType sql.SetType
}

var _ TypeInfo = (*setType)(nil)

func CreateSetTypeFromSqlSetType(sqlSetType sql.SetType) TypeInfo {
	return &setType{sqlSetType}
}

// Equals implements TypeInfo interface.
func (ti *setType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*setType); ok && ti.sqlSetType.NumberOfElements() == ti2.sqlSetType.NumberOfElements() {
		tiVals := ti.sqlSetType.Values()
		ti2Vals := ti2.sqlSetType.Values()
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
func (ti *setType) NomsKind() types.NomsKind {
	return types.UintKind
}

// String implements TypeInfo interface.
func (ti *setType) String() string {
	return fmt.Sprintf(`Set(Collation: %v, Values: %v)`, ti.sqlSetType.Collation().String(), strings.Join(ti.sqlSetType.Values(), ","))
}

// ToSqlType implements TypeInfo interface.
func (ti *setType) ToSqlType() sql.Type {
	return ti.sqlSetType
}
