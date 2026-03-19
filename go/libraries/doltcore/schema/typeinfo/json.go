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
	"github.com/dolthub/dolt/go/store/val"
)

type jsonType struct {
	jsonType sqltypes.JsonType
	enc      val.Encoding
}

var _ TypeInfo = (*jsonType)(nil)
var JSONType = &jsonType{jsonType: sqltypes.JsonType{}}

// Equals implements TypeInfo interface.
func (ti *jsonType) Equals(other TypeInfo) bool {
	if ti2, ok := other.(*jsonType); ok {
		return ti.Encoding() == ti2.Encoding()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *jsonType) NomsKind() types.NomsKind {
	return types.JSONKind
}

// String implements TypeInfo interface.
func (ti *jsonType) String() string {
	return "JSON"
}

// Encoding implements TypeInfo interface.
func (ti *jsonType) Encoding() val.Encoding {
	if ti.enc != 0 {
		return ti.enc
	}
	return val.JSONAddrEnc
}

// WithEncoding implements TypeInfo interface.
func (ti *jsonType) WithEncoding(enc val.Encoding) TypeInfo {
	return &jsonType{jsonType: ti.jsonType, enc: enc}
}

// ToSqlType implements TypeInfo interface.
func (ti *jsonType) ToSqlType() sql.Type {
	return ti.jsonType
}
