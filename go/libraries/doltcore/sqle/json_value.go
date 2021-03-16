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

package sqle

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

type NomsJSONValue struct {
	val types.Value
}

var _ sql.JSONValue = NomsJSONValue{}

func NomsJSONValueFromJSONDocument(val sql.JSONDocument) (NomsJSONValue, error) {
	panic("unimplemented")
}

func (v NomsJSONValue) Unmarshall() (val sql.JSONDocument, err error) {
	panic("unimplemented")
}

func (v NomsJSONValue) Compare(val sql.JSONValue) (cmp int, err error) {
	panic("unimplemented")
}

func (v NomsJSONValue) ToString() (string, error) {
	panic("unimplemented")
}
