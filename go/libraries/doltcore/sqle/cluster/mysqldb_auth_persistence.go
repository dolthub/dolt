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

package cluster

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

// mysqlDbAuthPersistence is the default AuthPersistence, which applies a
// replicated payload of serialized users and grants to the engine's
// mysql_db.MySQLDb instance and persists it locally. It is installed by
// Controller.HookMySQLDbPersister during standard Dolt server startup.
type mysqlDbAuthPersistence struct {
	mysqlDb *mysql_db.MySQLDb
}

var _ AuthPersistence = mysqlDbAuthPersistence{}

func (p mysqlDbAuthPersistence) SaveData(ctx *sql.Context, contents []byte) error {
	ed := p.mysqlDb.Editor()
	defer ed.Close()
	err := p.mysqlDb.OverwriteUsersAndGrantData(ctx, ed, contents)
	if err != nil {
		return err
	}
	// Persist goes through the persister registered with the MySQLDb, which
	// on a replication-enabled server is the controller's replicating
	// persister; on a standby the replicas hold the data without pushing it,
	// so this amounts to a local write.
	return p.mysqlDb.Persist(ctx, ed)
}
