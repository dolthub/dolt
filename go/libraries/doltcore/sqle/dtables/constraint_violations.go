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

package dtables

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/types"
)

// NewConstraintViolationsTable returns a sql.Table that lists constraint violations.
func NewConstraintViolationsTable(ctx *sql.Context, tblName doltdb.TableName, root doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	if root.VRW().Format() == types.Format_DOLT {
		return newProllyCVTable(ctx, tblName, root, rs)
	}

	panic("Unsupported format: " + root.VRW().Format().VersionString())
}
