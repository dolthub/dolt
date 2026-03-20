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

// This is a dolt implementation of the MySQL type Year, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type yearType struct {
	sqlYearType sql.YearType
	enc         val.Encoding
}

var _ TypeInfo = (*yearType)(nil)

var YearType = &yearType{sqlYearType: gmstypes.Year}

// Equals implements TypeInfo interface.
func (ti *yearType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*yearType); ok {
		return ti.Encoding() == ti2.Encoding()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *yearType) NomsKind() types.NomsKind {
	return types.IntKind
}

// String implements TypeInfo interface.
func (ti *yearType) String() string {
	return "Year"
}

// Encoding implements TypeInfo interface.
func (ti *yearType) Encoding() val.Encoding {
	if ti.enc != 0 {
		return ti.enc
	}
	return val.YearEnc
}

// WithEncoding implements TypeInfo interface.
func (ti *yearType) WithEncoding(enc val.Encoding) TypeInfo {
	if enc != val.YearEnc {
		panic(fmt.Errorf("encoding %v is not valid for %T", enc, ti))
	}
	return &yearType{sqlYearType: ti.sqlYearType, enc: enc}
}

// ToSqlType implements TypeInfo interface.
func (ti *yearType) ToSqlType() sql.Type {
	return ti.sqlYearType
}
