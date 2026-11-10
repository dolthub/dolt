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

package binlogreplication

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// checkDatabase returns an error if database |name| may not be binary logged. Dolt Enterprise replaces it.
var checkDatabase = func(name string, ddb *doltdb.DoltDB) error {
	return nil
}

// CheckDatabases returns an error if a database of |p| may not be binary logged.
func CheckDatabases(p *sqle.DoltDatabaseProvider) error {
	for _, db := range p.DoltDatabases() {
		if err := checkDatabase(db.Name(), db.DbData().Ddb); err != nil {
			return err
		}
	}
	return nil
}

// CheckDatabaseHook refuses to create a database that may not be binary logged. It must run before the other init
// hooks, so that a refused database leaves nothing behind.
func CheckDatabaseHook(ctx *sql.Context, _ *sqle.DoltDatabaseProvider, name string, denv *env.DoltEnv, _ dsess.SqlDatabase) error {
	return checkDatabase(name, denv.DoltDB(ctx))
}
