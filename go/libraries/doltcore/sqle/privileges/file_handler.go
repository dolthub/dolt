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

// serializePrivilegeTypes writes the given PrivilegeTypes into the flatbuffer Builder using the given flatbuffer start function, and returns the offset
// This helper function is used by PrivilegeSetColumn, PrivilegeSetTable, and PrivilegeSetDatabase
func serializePrivilegeTypes(b *flatbuffers.Builder, StartPTVector func(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT, pts []sql.PrivilegeType) flatbuffers.UOffsetT {
	// Order doesn't matter since it's a set of indexes
	StartPTVector(b, len(pts))
	for _, gs := range pts {
		b.PrependInt32(int32(gs))
	}
	return b.EndVector(len(pts))
}

// serializeVectorOffsets writes the given offsets slice to the flatbuffer Builder using the given start vector function, and returns the offset
func serializeVectorOffsets(b *flatbuffers.Builder, StartVector func(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT, offsets []flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	// Expect the given offsets slice to already be in reverse order
	StartVector(b, len(offsets))
	for _, offset := range offsets {
		b.PrependUOffsetT(offset)
	}
	return b.EndVector(len(offsets))
}

func serializeColumns(b *flatbuffers.Builder, columns []mysql_db.PrivilegeSetColumn) flatbuffers.UOffsetT {
	// Write column variables, and save offsets
	offsets := make([]flatbuffers.UOffsetT, len(columns))
	for i, column := range columns {
		name := b.CreateString(column.Name())
		privs := serializePrivilegeTypes(b, serial.PrivilegeSetColumnStartPrivsVector, column.ToSlice())

		serial.PrivilegeSetColumnStart(b)
		serial.PrivilegeSetColumnAddName(b, name)
		serial.PrivilegeSetColumnAddPrivs(b, privs)
		offsets[len(offsets)-i-1] = serial.PrivilegeSetColumnEnd(b) // reverse order
	}
	// Write column offsets (already reversed)
	return serializeVectorOffsets(b, serial.PrivilegeSetTableStartColumnsVector, offsets)
}

func serializeTables(b *flatbuffers.Builder, tables []mysql_db.PrivilegeSetTable) flatbuffers.UOffsetT {
	// Write table variables, and save offsets
	offsets := make([]flatbuffers.UOffsetT, len(tables))
	for i, table := range tables {
		name := b.CreateString(table.Name())
		privs := serializePrivilegeTypes(b, serial.PrivilegeSetTableStartPrivsVector, table.ToSlice())
		cols := serializeColumns(b, table.GetColumns())

		serial.PrivilegeSetTableStart(b)
		serial.PrivilegeSetTableAddName(b, name)
		serial.PrivilegeSetTableAddPrivs(b, privs)
		serial.PrivilegeSetTableAddColumns(b, cols)
		offsets[len(offsets)-i-1] = serial.PrivilegeSetTableEnd(b) // reverse order
	}
	// Write table offsets (order already reversed)
	return serializeVectorOffsets(b, serial.PrivilegeSetDatabaseStartTablesVector, offsets)
}

// serializeDatabases writes the given Privilege Set Databases into the flatbuffer Builder, and returns the offset
func serializeDatabases(b *flatbuffers.Builder, databases []mysql_db.PrivilegeSetDatabase) flatbuffers.UOffsetT {
	// Write database variables, and save offsets
	offsets := make([]flatbuffers.UOffsetT, len(databases))
	for i, database := range databases {
		name := b.CreateString(database.Name())
		privs := serializePrivilegeTypes(b, serial.PrivilegeSetDatabaseStartPrivsVector, database.ToSlice())
		tables := serializeTables(b, database.GetTables())

		serial.PrivilegeSetDatabaseStart(b)
		serial.PrivilegeSetDatabaseAddName(b, name)
		serial.PrivilegeSetDatabaseAddPrivs(b, privs)
		serial.PrivilegeSetDatabaseAddTables(b, tables)
		offsets[len(offsets)-i-1] = serial.PrivilegeSetDatabaseEnd(b)
	}

	// Write database offsets (order already reversed)
	return serializeVectorOffsets(b, serial.PrivilegeSetStartDatabasesVector, offsets)
}

func serializePrivilegeSet(b *flatbuffers.Builder, ps *mysql_db.PrivilegeSet) flatbuffers.UOffsetT {
	// Write privilege set variables, and save offsets
	globalStatic := serializePrivilegeTypes(b, serial.PrivilegeSetStartGlobalStaticVector, ps.ToSlice())
	// TODO: Serialize global_dynamic (it seems like it's currently not used?)
	databases := serializeDatabases(b, ps.GetDatabases())

	// Write PrivilegeSet
	serial.PrivilegeSetStart(b)
	serial.PrivilegeSetAddGlobalStatic(b, globalStatic)
	serial.PrivilegeSetAddDatabases(b, databases)
	return serial.PrivilegeSetEnd(b)
}

func serializeUser(b *flatbuffers.Builder, users []*mysql_db.User) flatbuffers.UOffsetT {
	// Write user variables, and save offsets
	offsets := make([]flatbuffers.UOffsetT, len(users))
	for i, user := range users {
		userName := b.CreateString(user.User)
		host := b.CreateString(user.Host)
		privilegeSet := serializePrivilegeSet(b, &user.PrivilegeSet)
		plugin := b.CreateString(user.Plugin)
		password := b.CreateString(user.Password)
		//attributes := b.CreateString(*user.Attributes)

		serial.UserStart(b)
		serial.UserAddUser(b, userName)
		serial.UserAddHost(b, host)
		serial.UserAddPrivilegeSet(b, privilegeSet)
		serial.UserAddPlugin(b, plugin)
		serial.UserAddPassword(b, password)
		serial.UserAddPasswordLastChanged(b, user.PasswordLastChanged.Unix())
		serial.UserAddLocked(b, user.Locked)
		//serial.UserAddAttributes(b, attributes)
		offsets[len(users)-i-1] = serial.UserEnd(b) // reverse order
	}

	// Write user offsets (already in reverse order)
	return serializeVectorOffsets(b, serial.MySQLDbStartUserVector, offsets)
}

func serializeRoleEdge(b *flatbuffers.Builder, roleEdges []*mysql_db.RoleEdge) flatbuffers.UOffsetT {
	offsets := make([]flatbuffers.UOffsetT, len(roleEdges))
	for i, roleEdge := range roleEdges {
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
		offsets[len(roleEdges)-i-1] = offset // reverse order
	}

	// Write role_edges vector (already in reversed order)
	return serializeVectorOffsets(b, serial.MySQLDbStartRoleEdgesVector, offsets)
}

// SaveData writes Catalog data to a flatbuffer file
// TODO: change input arguments
func SaveData(ctx *sql.Context, users []*mysql_db.User, roles []*mysql_db.RoleEdge) error {
	// TODO: just completely rewrite the whole thing I guess
	fileMutex.Lock()
	defer fileMutex.Unlock()

	b := flatbuffers.NewBuilder(0)
	user := serializeUser(b, users)
	roleEdge := serializeRoleEdge(b, roles)

	// Write MySQL DB
	serial.MySQLDbStart(b)
	serial.MySQLDbAddUser(b, user)
	serial.MySQLDbAddRoleEdges(b, roleEdge)
	mysqlDbOffset := serial.MySQLDbEnd(b)

	// Finish writing
	b.Finish(mysqlDbOffset)

	// Save to file
	return ioutil.WriteFile(mysqlDbFilePath, b.FinishedBytes(), 0777)
}
