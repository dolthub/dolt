// Copyright 2026 Dolthub, Inc.
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

package dprocedures

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// openPushRemote opens remote |r|, which |local| is pushed or synced to.
var openPushRemote = func(ctx *sql.Context, p dsess.DoltDatabaseProvider, local *doltdb.DoltDB, r env.Remote) (*doltdb.DoltDB, error) {
	return p.GetRemoteDB(ctx, local.Format(), r)
}

// newRestoreCreator returns what creates the database that |backup| is restored into.
var newRestoreCreator = func(ctx *sql.Context, p dsess.DoltDatabaseProvider, backup *doltdb.DoltDB) (func(*sql.Context, string) error, error) {
	return p.CreateDatabase, nil
}
