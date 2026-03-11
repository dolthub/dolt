// Copyright 2022 Dolthub, Inc.
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

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type multipointType struct {
	sqlMultiPointType gmstypes.MultiPointType
}

var _ TypeInfo = (*multipointType)(nil)

var MultiPointType = &multipointType{gmstypes.MultiPointType{}}

// Equals implements TypeInfo interface.
func (ti *multipointType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*multipointType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return (!ti.sqlMultiPointType.DefinedSRID && !o.sqlMultiPointType.DefinedSRID) || ti.sqlMultiPointType.SRID == o.sqlMultiPointType.SRID
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *multipointType) NomsKind() types.NomsKind {
	return types.MultiPointKind
}

// String implements TypeInfo interface.
func (ti *multipointType) String() string {
	return "multipoint"
}

// ToSqlType implements TypeInfo interface.
func (ti *multipointType) ToSqlType() sql.Type {
	return ti.sqlMultiPointType
}
