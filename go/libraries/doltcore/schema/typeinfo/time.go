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

// This is a dolt implementation of the MySQL type Time, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type timeType struct {
	sqlTimeType gmstypes.TimeType
}

var _ TypeInfo = (*timeType)(nil)

var TimeType = &timeType{gmstypes.Time}

// Equals implements TypeInfo interface.
func (ti *timeType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*timeType)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *timeType) NomsKind() types.NomsKind {
	return types.IntKind
}

// String implements TypeInfo interface.
func (ti *timeType) String() string {
	return "Time"
}

// ToSqlType implements TypeInfo interface.
func (ti *timeType) ToSqlType() sql.Type {
	return ti.sqlTimeType
}
