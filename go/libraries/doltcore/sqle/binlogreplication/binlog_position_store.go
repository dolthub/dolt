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
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const binlogPositionFilename = "binlog-position"
const mysqlFlavor = "MySQL56"

// binlogPositionStore manages loading and saving data to the binlog position file stored on disk. This provides
// durable storage for the set of GTIDs that have been successfully executed on the replica, so that the replica
// server can be restarted and resume binlog event messages at the correct point.
type binlogPositionStore struct {
	mu sync.Mutex
}

// Load loads a mysql.Position instance from the .dolt/binlog-position file at the root of the provider's filesystem.
// This file must be stored at the root of the provider's filesystem, and NOT inside a nested database's .dolt directory,
// since the binlog position contains events that cover all databases in a SQL server. The returned mysql.Position
// represents the set of GTIDs that have been successfully executed and applied on this replica. Currently only the
// default binlog channel ("") is supported. If no .dolt/binlog-position file is stored, this method returns a nil
// mysql.Position and a nil error. If any errors are encountered, a nil mysql.Position and an error are returned.
func (store *binlogPositionStore) Load(ctx *sql.Context) (*mysql.Position, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	doltSession := dsess.DSessFromSess(ctx.Session)
	filesys := doltSession.Provider().FileSystem()

	doltDirExists, _ := filesys.Exists(dbfactory.DoltDir)
	if !doltDirExists {
		return nil, nil
	}

	positionFileExists, _ := filesys.Exists(filepath.Join(dbfactory.DoltDir, binlogPositionFilename))
	if !positionFileExists {
		return nil, nil
	}

	filePath, err := filesys.Abs(filepath.Join(dbfactory.DoltDir, binlogPositionFilename))
	if err != nil {
		return nil, err
	}

	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	positionString := string(bytes)

	// Strip off the "MySQL56/" prefix
	prefix := "MySQL56/"
	if strings.HasPrefix(positionString, prefix) {
		positionString = string(bytes[len(prefix):])
	}

	position, err := mysql.ParsePosition(mysqlFlavor, positionString)
	if err != nil {
		return nil, err
	}

	return &position, nil
}

// Save saves the specified |position| to disk in the .dolt/binlog-position file at the root of the provider's
// filesystem. This file must be stored at the root of the provider's filesystem, and NOT inside a nested database's
// .dolt directory, since the binlog position contains events that cover all databases in a SQL server. |position|
// represents the set of GTIDs that have been successfully executed and applied on this replica. Currently only the
// default binlog channel ("") is supported. If any errors are encountered persisting the position to disk, an
// error is returned.
func (store *binlogPositionStore) Save(ctx *sql.Context, position *mysql.Position) error {
	if position == nil {
		return fmt.Errorf("unable to save binlog position: nil position passed")
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	doltSession := dsess.DSessFromSess(ctx.Session)
	filesys := doltSession.Provider().FileSystem()

	// The .dolt dir may not exist yet if a dolt sql-server was started without dolt init'ing
	// the root directory, so create it if necessary.
	exists, isDir := filesys.Exists(dbfactory.DoltDir)
	if !exists {
		err := filesys.MkDirs(dbfactory.DoltDir)
		if err != nil {
			return fmt.Errorf("unable to save binlog position: %s", err)
		}
	} else if !isDir {
		return fmt.Errorf("unable to save binlog position: .dolt exists as a file, not a dir")
	}

	filePath, err := filesys.Abs(filepath.Join(dbfactory.DoltDir, binlogPositionFilename))
	if err != nil {
		return err
	}

	encodedPosition := mysql.EncodePosition(*position)
	return os.WriteFile(filePath, []byte(encodedPosition), 0666)
}

// Delete deletes the stored mysql.Position information stored in .dolt/binlog-position in the root of the provider's
// filesystem. This is useful for the "RESET REPLICA" command, since it clears out the current replication state. If
// any errors are encountered removing the position file, an error is returned.
func (store *binlogPositionStore) Delete(ctx *sql.Context) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	doltSession := dsess.DSessFromSess(ctx.Session)
	filesys := doltSession.Provider().FileSystem()

	return filesys.Delete(filepath.Join(dbfactory.DoltDir, binlogPositionFilename), false)
}
