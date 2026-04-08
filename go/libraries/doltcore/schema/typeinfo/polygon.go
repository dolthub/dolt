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

	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// This is a dolt implementation of the MySQL type Point, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type polygonType struct {
	sqlPolygonType gmstypes.PolygonType
	enc            val.Encoding
}

var _ TypeInfo = (*polygonType)(nil)

var PolygonType = &polygonType{sqlPolygonType: gmstypes.PolygonType{}}

// Equals implements TypeInfo interface.
func (ti *polygonType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*polygonType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return ((!ti.sqlPolygonType.DefinedSRID && !o.sqlPolygonType.DefinedSRID) || ti.sqlPolygonType.SRID == o.sqlPolygonType.SRID) &&
			ti.Encoding() == o.Encoding()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *polygonType) NomsKind() types.NomsKind {
	return types.PolygonKind
}

// String implements TypeInfo interface.
func (ti *polygonType) String() string {
	return "Polygon"
}

// Encoding implements TypeInfo interface.
func (ti *polygonType) Encoding() val.Encoding {
	if ti.enc != 0 {
		return ti.enc
	}
	if UseAdaptiveEncoding {
		return val.GeomAdaptiveEnc
	}
	return val.GeomAddrEnc
}

// WithEncoding implements TypeInfo interface.
func (ti *polygonType) WithEncoding(enc val.Encoding) TypeInfo {
	if enc != val.GeomAddrEnc && enc != val.GeometryEnc && enc != val.GeomAdaptiveEnc {
		panic(fmt.Errorf("encoding %v is not valid for %T", enc, ti))
	}
	return &polygonType{sqlPolygonType: ti.sqlPolygonType, enc: enc}
}

// ToSqlType implements TypeInfo interface.
func (ti *polygonType) ToSqlType() sql.Type {
	return ti.sqlPolygonType
}
