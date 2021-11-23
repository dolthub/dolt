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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
)

func newRowConverter(sqlSch sql.Schema, sch schema.Schema, kd, vd val.TupleDesc) (rc rowConv) {
	rc = rowConv{
		keyMap: nil,
		valMap: nil,
		keyBld: val.TupleBuilder{},
		valBld: val.TupleBuilder{},
	}
	return
}

type rowConv struct {
	keyMap, valMap []int
	keyBld, valBld val.TupleBuilder
}

func (rc rowConv) ConvertRow(row sql.Row) (key, value val.Tuple) {
	for i, j := range rc.keyMap {
		rc.keyBld.PutField(i, row[j])
	}
	key = rc.keyBld.Build(shimPool)

	for i, j := range rc.valMap {
		rc.valBld.PutField(i, row[j])
	}
	value = rc.valBld.Build(shimPool)

	return
}
