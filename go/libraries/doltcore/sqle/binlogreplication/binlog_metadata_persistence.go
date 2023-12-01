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

package binlogreplication

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
)

// persistReplicationConfiguration saves the specified |replicaSourceInfo| to disk; if any problems are encountered
// while saving to disk, an error is returned.
func persistReplicationConfiguration(ctx *sql.Context, replicaSourceInfo *mysql_db.ReplicaSourceInfo) error {
	server := sqlserver.GetRunningServer()
	if server == nil {
		return fmt.Errorf("no SQL server running; " +
			"replication commands may only be used when running from dolt sql-server, and not from dolt sql")
	}
	engine := server.Engine

	mysqlDb := engine.Analyzer.Catalog.MySQLDb
	ed := mysqlDb.Editor()
	defer ed.Close()
	ed.PutReplicaSourceInfo(replicaSourceInfo)
	return mysqlDb.Persist(ctx, ed)
}

// loadReplicationConfiguration loads the replication configuration for default channel ("").
func loadReplicationConfiguration(_ *sql.Context) (*mysql_db.ReplicaSourceInfo, error) {
	server := sqlserver.GetRunningServer()
	if server == nil {
		return nil, fmt.Errorf("no SQL server running; " +
			"replication commands may only be used when running from dolt sql-server, and not from dolt sql")
	}
	engine := server.Engine
	mysqlDb := engine.Analyzer.Catalog.MySQLDb
	rd := mysqlDb.Reader()
	defer rd.Close()

	rsi, ok := rd.GetReplicaSourceInfo(mysql_db.ReplicaSourceInfoPrimaryKey{
		Channel: "",
	})
	if ok {
		return rsi, nil
	}

	return nil, nil
}

// deleteReplicationConfiguration deletes all replication configuration for the default channel ("").
func deleteReplicationConfiguration(ctx *sql.Context) error {
	server := sqlserver.GetRunningServer()
	if server == nil {
		return fmt.Errorf("no SQL server running; " +
			"replication commands may only be used when running from dolt sql-server, and not from dolt sql")
	}
	engine := server.Engine
	mysqlDb := engine.Analyzer.Catalog.MySQLDb
	ed := mysqlDb.Editor()
	defer ed.Close()

	ed.RemoveReplicaSourceInfo(mysql_db.ReplicaSourceInfoPrimaryKey{})

	return engine.Analyzer.Catalog.MySQLDb.Persist(ctx, ed)
}

// persistSourceUuid saves the specified |sourceUuid| to a persistent storage location.
func persistSourceUuid(ctx *sql.Context, sourceUuid string) error {
	replicaSourceInfo, err := loadReplicationConfiguration(ctx)
	if err != nil {
		return err
	}

	replicaSourceInfo.Uuid = sourceUuid
	return persistReplicationConfiguration(ctx, replicaSourceInfo)
}
