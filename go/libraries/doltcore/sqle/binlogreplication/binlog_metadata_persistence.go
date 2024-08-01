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
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

// replicationRunningStateDirectory is the directory where the "replica-running" file is stored to indicate that
// replication on a replica was running the last time the server was running.
const replicationRunningStateDirectory = ".doltcfg"

// replicaRunningFilename holds the name of the file that indicates replication was running on a replica server.
const replicaRunningFilename = "replica-running"

// replicaRunningState indicates if a replica was actively running replication.
type replicaRunningState int

const (
	running replicaRunningState = iota
	notRunning
)

// persistReplicationConfiguration saves the specified |replicaSourceInfo| to the "mysql"
// database |mysqlDb|. If any problems are encountered while saving to disk, an error is returned.
func persistReplicationConfiguration(ctx *sql.Context, replicaSourceInfo *mysql_db.ReplicaSourceInfo, mysqlDb *mysql_db.MySQLDb) error {
	ed := mysqlDb.Editor()
	defer ed.Close()
	ed.PutReplicaSourceInfo(replicaSourceInfo)
	return mysqlDb.Persist(ctx, ed)
}

// loadReplicationRunningState loads the replication running state from disk by looking for a "replica-running" file
// in the .doltcfg directory. An error is returned if any problems were encountered loading the state from disk.
func loadReplicationRunningState(ctx *sql.Context) (replicaRunningState, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	filesys := doltSession.Provider().FileSystem()

	replicationRunningStateFilepath, err := filesys.Abs(
		filepath.Join(replicationRunningStateDirectory, replicaRunningFilename))
	if err != nil {
		return notRunning, err
	}

	if replicaRunning, _ := filesys.Exists(replicationRunningStateFilepath); replicaRunning {
		return running, nil
	} else {
		return notRunning, nil
	}
}

// persistReplicaRunningState records the running |state| of a replica to disk by creating a "replica-running" empty
// file in the .doltcfg directory. An error is returned if any problems were encountered saving the state to disk.
func persistReplicaRunningState(ctx *sql.Context, state replicaRunningState) error {
	doltSession := dsess.DSessFromSess(ctx.Session)
	filesys := doltSession.Provider().FileSystem()

	// The .doltcfg dir may not exist yet, so create it if necessary.
	err := createDoltCfgDir(filesys)
	if err != nil {
		return err
	}

	replicationRunningStateFilepath, err := filesys.Abs(
		filepath.Join(replicationRunningStateDirectory, replicaRunningFilename))
	if err != nil {
		return err
	}

	switch state {
	case running:
		return createEmptyFile(replicationRunningStateFilepath)
	case notRunning:
		_, err = os.Stat(replicationRunningStateFilepath)
		if os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return err
		}
		return os.Remove(replicationRunningStateFilepath)
	default:
		return fmt.Errorf("unsupported replica running state: %v", state)
	}
}

// loadReplicationConfiguration loads the replication configuration for default channel ("") from
// the "mysql" database, |mysqlDb|.
func loadReplicationConfiguration(_ *sql.Context, mysqlDb *mysql_db.MySQLDb) (*mysql_db.ReplicaSourceInfo, error) {
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

// deleteReplicationConfiguration deletes all replication configuration for the default channel ("")
// from the specified "mysql" database, |mysqlDb|.
func deleteReplicationConfiguration(ctx *sql.Context, mysqlDb *mysql_db.MySQLDb) error {
	ed := mysqlDb.Editor()
	defer ed.Close()

	ed.RemoveReplicaSourceInfo(mysql_db.ReplicaSourceInfoPrimaryKey{})

	return mysqlDb.Persist(ctx, ed)
}

// persistSourceUuid saves the specified |sourceUuid| to the "mysql" database, |mysqlDb|.
func persistSourceUuid(ctx *sql.Context, sourceUuid string, mysqlDb *mysql_db.MySQLDb) error {
	replicaSourceInfo, err := loadReplicationConfiguration(ctx, mysqlDb)
	if err != nil {
		return err
	}

	replicaSourceInfo.Uuid = sourceUuid
	return persistReplicationConfiguration(ctx, replicaSourceInfo, mysqlDb)
}

// createEmptyFile creates an empty file at |fullFilepath| if a file does not exist already. If a file does exist
// at that path, no action is taken.
func createEmptyFile(fullFilepath string) (err error) {
	_, err = os.Stat(fullFilepath)
	if os.IsNotExist(err) {
		var emptyFile *os.File
		emptyFile, err = os.Create(fullFilepath)
		defer emptyFile.Close()
	}
	return err
}
