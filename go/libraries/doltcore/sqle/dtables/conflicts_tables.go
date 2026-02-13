package dtables

// Copyright 2019 Dolthub, Inc.
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

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/types"
)

// NewConflictsTable returns a new ConflictsTable instance
func NewConflictsTable(ctx *sql.Context, tblName doltdb.TableName, srcTable sql.Table, root doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	var tbl *doltdb.Table
	var err error
	tbl, tblName, err = getTableInsensitiveOrError(ctx, root, tblName)
	if err != nil {
		return nil, err
	}

	types.AssertFormat_DOLT(tbl.Format())
	upd, ok := srcTable.(sql.UpdatableTable)
	if !ok {
		return nil, fmt.Errorf("%s can not have conflicts because it is not updateable", tblName)
	}
	return newProllyConflictsTable(ctx, tbl, upd, tblName, root, rs)
}
