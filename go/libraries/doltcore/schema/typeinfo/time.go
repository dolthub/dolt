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
	"github.com/dolthub/dolt/go/store/val"
)

// This is a dolt implementation of the MySQL type Time, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type timeType struct {
	sqlTimeType gmstypes.TimeType
	enc         val.Encoding
}

var _ TypeInfo = (*timeType)(nil)

var TimeType = &timeType{sqlTimeType: gmstypes.Time}

// Equals implements TypeInfo interface.
func (ti *timeType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*timeType); ok {
		return ti.Encoding() == ti2.Encoding()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *timeType) NomsKind() types.NomsKind {
	return types.IntKind
}

// String implements TypeInfo interface.
func (ti *timeType) String() string {
	return "Time"
}

// Encoding implements TypeInfo interface.
func (ti *timeType) Encoding() val.Encoding {
	if ti.enc != 0 {
		return ti.enc
	}
	return val.TimeEnc
}

// WithEncoding implements TypeInfo interface.
func (ti *timeType) WithEncoding(enc val.Encoding) TypeInfo {
	return &timeType{sqlTimeType: ti.sqlTimeType, enc: enc}
}

// ToSqlType implements TypeInfo interface.
func (ti *timeType) ToSqlType() sql.Type {
	return ti.sqlTimeType
}
