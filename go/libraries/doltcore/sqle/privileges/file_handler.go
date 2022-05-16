// Copyright 2022 Dolthub, Inc.
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

package privileges

import (
	"encoding/json"
	"errors"
	flatbuffers "github.com/google/flatbuffers/go"
	"io/ioutil"
	"os"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/mysql_db/serial"
)

var (
	filePath  string
	fileMutex = &sync.Mutex{}
)

// TODO: decide on the right path
const mysqlDbFilePath = "mysql.db"

// privDataJson is used to marshal/unmarshal the privilege data to/from JSON.
type privDataJson struct {
	Users []*mysql_db.User
	Roles []*mysql_db.RoleEdge
}

// SetFilePath sets the file path that will be used for saving and loading privileges.
func SetFilePath(fp string) {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	_, err := os.Stat(fp)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := ioutil.WriteFile(fp, []byte{}, 0644); err != nil {
				// If we can't create the file it's a catastrophic error
				panic(err)
			}
		} else {
			// Some strange unknown failure, okay to panic here
			panic(err)
		}
	}
	filePath = fp
}

// LoadPrivileges reads the file previously set on the file path and returns the privileges and role connections. If the
// file path has not been set, returns an empty slice for both, but does not error. This is so that the logic path can
// retain the calls regardless of whether a user wants privileges to be loaded or persisted.
func LoadPrivileges() ([]*mysql_db.User, []*mysql_db.RoleEdge, error) {
	fileMutex.Lock()
	defer fileMutex.Unlock()
	if filePath == "" {
		return nil, nil, nil
	}

	fileContents, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}
	if len(fileContents) == 0 {
		return nil, nil, nil
	}
	data := &privDataJson{}
	err = json.Unmarshal(fileContents, data)
	if err != nil {
		return nil, nil, err
	}
	return data.Users, data.Roles, nil
}

// LoadData reads the mysql.db file, returns nil if empty or not found
func LoadData() (*serial.MySQLDb, error) {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	// TODO: right filepath?
	buf, err := ioutil.ReadFile(mysqlDbFilePath)
	if err != nil {
		return nil, nil
	}
	if len(buf) == 0 {
		return nil, nil
	}

	// TODO: Flat buffers?
	mysqlDb := serial.GetRootAsMySQLDb(buf, 0)

	return mysqlDb, nil
}

var _ mysql_db.PrivilegePersistCallback = SavePrivileges
var _ mysql_db.DataPersistCallback = SaveData

// SavePrivileges implements the interface mysql_db.PrivilegePersistCallback. This is used to save privileges to disk. If the
// file path has not been previously set, this returns without error. This is so that the logic path can retain the
// calls regardless of whether a user wants privileges to be loaded or persisted.
func SavePrivileges(ctx *sql.Context, users []*mysql_db.User, roles []*mysql_db.RoleEdge) error {
	fileMutex.Lock()
	defer fileMutex.Unlock()
	if filePath == "" {
		return nil
	}

	data := &privDataJson{
		Users: users,
		Roles: roles,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filePath, jsonData, 0777)
}

func SaveData(ctx *sql.Context, users []*mysql_db.User, roles []*mysql_db.RoleEdge) error {
	// TODO: just completely rewrite the whole thing I guess
	fileMutex.Lock()
	defer fileMutex.Unlock()

	b := flatbuffers.NewBuilder(0)

	// TODO: write users

	// This is needlessly complicated...
	// Start writing role_edges
	serial.MySQLDbStartRoleEdgesVector(b, len(roles))
	for _, roleEdge := range roles {
		// Serialize each of the member vars in RoleEdge and save their offsets
		fromHost := b.CreateString(roleEdge.FromHost)
		fromUser := b.CreateString(roleEdge.FromUser)
		toHost := b.CreateString(roleEdge.ToHost)
		toUser := b.CreateString(roleEdge.ToUser)

		// Write their offsets to flatbuffer builder
		serial.RoleEdgeAddFromHost(b, fromHost)
		serial.RoleEdgeAddFromUser(b, fromUser)
		serial.RoleEdgeAddToHost(b, toHost)
		serial.RoleEdgeAddToUser(b, toUser)

		// Write WithAdminOption (boolean value doesn't need offset)
		serial.RoleEdgeAddWithAdminOption(b, roleEdge.WithAdminOption)
	}
	// Save where the vector ends for mysql db
	roleEdgeOffset := b.EndVector(len(roles))

	// Start writing the MySQL DB
	serial.MySQLDbStart(b)
	serial.MySQLDbAddRoleEdges(b, roleEdgeOffset)

	return ioutil.WriteFile(mysqlDbFilePath, jsonData, 0777)
}
