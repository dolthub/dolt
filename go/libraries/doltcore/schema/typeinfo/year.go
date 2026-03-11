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

	"github.com/dolthub/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Year, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type yearType struct {
	sqlYearType sql.YearType
}

var _ TypeInfo = (*yearType)(nil)

var YearType = &yearType{gmstypes.Year}

// Equals implements TypeInfo interface.
func (ti *yearType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*yearType)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *yearType) NomsKind() types.NomsKind {
	return types.IntKind
}

// String implements TypeInfo interface.
func (ti *yearType) String() string {
	return "Year"
}

// ToSqlType implements TypeInfo interface.
func (ti *yearType) ToSqlType() sql.Type {
	return ti.sqlYearType
}
