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
)

const (
	datetimeTypeParam_SQL           = "sql"
	datetimeTypeParam_SQL_Date      = "date"
	datetimeTypeParam_SQL_Datetime  = "datetime"
	datetimeTypeParam_SQL_Timestamp = "timestamp"
	datetimeTypeParam_Precision     = "precision"
)

type datetimeType struct {
	sqlDatetimeType sql.DatetimeType
}

var _ TypeInfo = (*datetimeType)(nil)
var (
	DateType      = &datetimeType{gmstypes.Date}
	DatetimeType  = &datetimeType{gmstypes.DatetimeMaxPrecision}
	TimestampType = &datetimeType{gmstypes.TimestampMaxPrecision}
)

func CreateDatetimeTypeFromSqlType(typ sql.DatetimeType) *datetimeType {
	return &datetimeType{typ}
}

// Equals implements TypeInfo interface.
func (ti *datetimeType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*datetimeType); ok {
		return ti.sqlDatetimeType.Type() == ti2.sqlDatetimeType.Type()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *datetimeType) NomsKind() types.NomsKind {
	return types.TimestampKind
}

// String implements TypeInfo interface.
func (ti *datetimeType) String() string {
	return fmt.Sprintf(`Datetime(SQL: "%v")`, ti.sqlDatetimeType.String())
}

// ToSqlType implements TypeInfo interface.
func (ti *datetimeType) ToSqlType() sql.Type {
	return ti.sqlDatetimeType
}
