// Copyright 2023 Dolthub, Inc.
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
)

// VersionableTable is a sql.Table that has a history. The history can be queried by setting a specific doltdb.RootValue.
type VersionableTable interface {
	sql.Table
	LockedToRoot(ctx *sql.Context, root doltdb.RootValue) (sql.IndexAddressableTable, error)
}

// RootSetter is an interface that can be used to set the root of a working set.
type RootSetter interface {
	SetRoot(ctx *sql.Context, root doltdb.RootValue) error
}
