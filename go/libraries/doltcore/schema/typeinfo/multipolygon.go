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

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type multipolygonType struct {
	sqlMultiPolygonType gmstypes.MultiPolygonType
}

var _ TypeInfo = (*multipolygonType)(nil)

var MultiPolygonType = &multipolygonType{gmstypes.MultiPolygonType{}}

// Equals implements TypeInfo interface.
func (ti *multipolygonType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*multipolygonType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return (!ti.sqlMultiPolygonType.DefinedSRID && !o.sqlMultiPolygonType.DefinedSRID) || ti.sqlMultiPolygonType.SRID == o.sqlMultiPolygonType.SRID
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *multipolygonType) NomsKind() types.NomsKind {
	return types.MultiPolygonKind
}

// String implements TypeInfo interface.
func (ti *multipolygonType) String() string {
	return "MultiPolygon"
}

// ToSqlType implements TypeInfo interface.
func (ti *multipolygonType) ToSqlType() sql.Type {
	return ti.sqlMultiPolygonType
}
