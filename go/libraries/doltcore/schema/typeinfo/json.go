// Copyright 2021 Dolthub, Inc.
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
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

type jsonType struct {
	jsonType sqltypes.JsonType
}

var _ TypeInfo = (*jsonType)(nil)
var JSONType = &jsonType{sqltypes.JsonType{}}

// Equals implements TypeInfo interface.
func (ti *jsonType) Equals(other TypeInfo) bool {
	_, ok := other.(*jsonType)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *jsonType) NomsKind() types.NomsKind {
	return types.JSONKind
}

// String implements TypeInfo interface.
func (ti *jsonType) String() string {
	return "JSON"
}

// ToSqlType implements TypeInfo interface.
func (ti *jsonType) ToSqlType() sql.Type {
	return ti.jsonType
}
