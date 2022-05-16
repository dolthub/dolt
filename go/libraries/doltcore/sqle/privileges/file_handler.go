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

// SaveData writes Catalog data to a flatbuffer file
// TODO: change input arguments
func SaveData(ctx *sql.Context, users []*mysql_db.User, roles []*mysql_db.RoleEdge) error {
	// TODO: just completely rewrite the whole thing I guess
	fileMutex.Lock()
	defer fileMutex.Unlock()

	b := flatbuffers.NewBuilder(0)

	// TODO: write users

	// Serialize users
	userOffsets := make([]flatbuffers.UOffsetT, len(users))
	for i, user := range users {
		// Serialize global_static; order doesn't matter since it's a set of indexes
		globalStatics := user.PrivilegeSet.ToSlice()
		serial.PrivilegeSetStartGlobalStaticVector(b, len(globalStatics))
		for _, globalStatic := range globalStatics {
			b.PrependInt32(int32(globalStatic))
		}
		globalStaticVectorOffset := b.EndVector(len(globalStatics))

		// TODO: Serialize global_dynamic

		// Serialize databases
		databases := user.PrivilegeSet.GetDatabases()
		for _, db := range databases {
			// Serialize database name
			dbNameOffset := b.CreateString(db.Name())

			// Serialize database privs
			dbPrivs := db.ToSlice()
			serial.PrivilegeSetDatabaseStartPrivsVector(b, len(dbPrivs))
			for _, priv := range dbPrivs {
				b.PrependInt32(int32(priv))
			}
			dbPrivsOffset := b.EndVector(len(dbPrivs))

			// Serialize database tables
			tables := db.GetTables()
			tableOffsets := make([]flatbuffers.UOffsetT, len(tables))
			for _, table := range tables {
				// Serialize table name
				tableNameOffset := b.CreateString(table.Name())

				// Serialize table privs
				tablePrivs := table.ToSlice()
				serial.PrivilegeSetTableStartPrivsVector(b, len(tablePrivs))
				for _, priv := range tablePrivs {
					b.PrependInt32(int32(priv))
				}
				tablePrivsOffset := b.EndVector(len(tablePrivs))

				// Serialize table columns
				tableCols := table.GetColumns()
				tableColOffsets := make([]flatbuffers.UOffsetT, len(tableCols))
				for i, col := range tableCols {
					// Serialize column name
					colNameOffset := b.CreateString(col.Name())

					// Serialize column privs
					colPrivs := col.ToSlice()
					for _, priv := range colPrivs {
						b.PrependInt32(int32(priv))
					}
					colPrivsOffset := b.EndVector(len(colPrivs))

					// Write column, and save offset
					serial.PrivilegeSetColumnStart(b)
					serial.PrivilegeSetColumnAddName(b, colNameOffset)
					serial.PrivilegeSetColumnAddPrivs(b, colPrivsOffset)
					tableColOffsets[i] = serial.PrivilegeSetColumnEnd(b)
				}

				// Write table column offsets
				serial.PrivilegeSetTableStartColumnsVector(b, len(tableColOffsets))
				for _, colOffset := range tableColOffsets {
					b.PrependUOffsetT(colOffset)
				}
				tableColsOffset := b.EndVector(len(tableColOffsets))

				// Write table, and save offset
				serial.PrivilegeSetTableStart(b)
				serial.PrivilegeSetTableAddName(b, tableNameOffset)
				serial.PrivilegeSetTableAddPrivs(b, tablePrivsOffset)
				serial.PrivilegeSetTableAddColumns(b, tableColsOffset)
				tableOffsets[i] = serial.PrivilegeSetTableEnd(b)
			}

			// Serialize table

			serial.PrivilegeSetDatabaseStart(b)
			serial.PrivilegeSetDatabaseAddName(b, dbNameOffset)
			serial.PrivilegeSetDatabaseAddPrivs(b, dbPrivsOffset)
			serial.PrivilegeSetDatabaseAddTables(b, tableOffsets)
			serial.PrivilegeSetDatabaseEnd(b)
		}

		privSetDbPrivs := []int{}

		serial.PrivilegeSetStartDatabasesVector(b)

		serial.PrivilegeSetStart(b)
		serial.PrivilegeSetEnd(b)
		privilegeSet := 0

		// Serialize string member variables
		userName := b.CreateString(user.User)
		host := b.CreateString(user.Host)
		plugin := b.CreateString(user.Plugin)
		password := b.CreateString(user.Password)
		attributes := b.CreateString(*user.Attributes)

		// Write user, and save offset
		serial.UserStart(b)
		serial.UserAddUser(b, userName)
		serial.UserAddHost(b, host)
		serial.UserAddPrivilegeSet(b)
		serial.UserAddPlugin(b, plugin)
		serial.UserAddPassword(b, password)
		serial.UserAddAttributes(b, attributes)
		userOffsets[len(users)-i+1] = serial.UserEnd(b)
	}

	serial.MySQLDbStartUserVector(b, len(users))

	// Serialize role_edges
	roleEdgeOffsets := make([]flatbuffers.UOffsetT, len(roles))
	for i, roleEdge := range roles {
		// Serialize each of the member vars in RoleEdge and save their offsets
		fromHost := b.CreateString(roleEdge.FromHost)
		fromUser := b.CreateString(roleEdge.FromUser)
		toHost := b.CreateString(roleEdge.ToHost)
		toUser := b.CreateString(roleEdge.ToUser)

		// Start RoleEdge
		serial.RoleEdgeStart(b)

		// Write their offsets to flatbuffer builder
		serial.RoleEdgeAddFromHost(b, fromHost)
		serial.RoleEdgeAddFromUser(b, fromUser)
		serial.RoleEdgeAddToHost(b, toHost)
		serial.RoleEdgeAddToUser(b, toUser)

		// Write WithAdminOption (boolean value doesn't need offset)
		serial.RoleEdgeAddWithAdminOption(b, roleEdge.WithAdminOption)

		// End RoleEdge
		offset := serial.RoleEdgeEnd(b)
		roleEdgeOffsets[len(roles)-i+1] = offset // reverse order
	}

	// Write role_edges vector; slice is already reversed
	serial.MySQLDbStartRoleEdgesVector(b, len(roles))
	for _, offset := range roleEdgeOffsets {
		b.PrependUOffsetT(offset)
	}
	roleEdgeVectorOffset := b.EndVector(len(roles))

	// Write MySQL DB
	serial.MySQLDbStart(b)
	serial.MySQLDbAddRoleEdges(b, roleEdgeVectorOffset)
	mysqlDbOffset := serial.MySQLDbEnd(b)
	b.Finish(mysqlDbOffset)

	// Save to file
	buf := b.FinishedBytes()
	return ioutil.WriteFile(mysqlDbFilePath, buf, 0777)
}
