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

package mysql_file_handler

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

const defaultMySQLFilePath = "mysql.db"

var fileMutex = &sync.Mutex{}
var mysqlDbFilePath string
var privsFilePath string

// privDataJson is used to marshal/unmarshal the privilege data to/from JSON.
type privDataJson struct {
	Users []*mysql_db.User
	Roles []*mysql_db.RoleEdge
}

// SetPrivilegeFilePath sets the file path that will be used for loading privileges.
func SetPrivilegeFilePath(fp string) {
	// do nothing for empty file path
	if len(fp) == 0 {
		return
	}

	fileMutex.Lock()
	defer fileMutex.Unlock()

	// Panic if some strange unknown failure occurs (not just that it doesn't exist)
	_, err := os.Stat(fp)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}
	privsFilePath = fp
}

// SetMySQLDbFilePath sets the file path that will be used for saving and loading MySQL Db tables.
func SetMySQLDbFilePath(fp string) {
	// look for default mysql db file path if none specified
	if len(fp) == 0 {
		fp = defaultMySQLFilePath
	}

	fileMutex.Lock()
	defer fileMutex.Unlock()

	// Panic if some strange unknown failure occurs (not just that it doesn't exist)
	_, err := os.Stat(fp)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}
	mysqlDbFilePath = fp
}

// LoadPrivileges reads the file previously set on the file path and returns the privileges and role connections. If the
// file path has not been set, returns an empty slice for both, but does not error. This is so that the logic path can
// retain the calls regardless of whether a user wants privileges to be loaded or persisted.
func LoadPrivileges() ([]*mysql_db.User, []*mysql_db.RoleEdge, error) {
	// return nil for empty path
	if len(privsFilePath) == 0 {
		return nil, nil, nil
	}

	fileMutex.Lock()
	defer fileMutex.Unlock()

	// read from privsFilePath, error if something other than not-exists
	fileContents, err := ioutil.ReadFile(privsFilePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
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
func LoadData() ([]byte, error) {
	// use default mysql db file path if none specified
	if len(mysqlDbFilePath) == 0 {
		mysqlDbFilePath = defaultMySQLFilePath
	}

	fileMutex.Lock()
	defer fileMutex.Unlock()

	// read from mysqldbFilePath, error if something other than not-exists
	buf, err := ioutil.ReadFile(mysqlDbFilePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, nil
	}

	return buf, nil
}

var _ mysql_db.PersistCallback = SaveData

// SaveData writes the provided []byte (in valid flatbuffer format) to the mysql db file
func SaveData(ctx *sql.Context, data []byte) error {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	// use default if empty filepath
	if len(mysqlDbFilePath) == 0 {
		mysqlDbFilePath = defaultMySQLFilePath
	}

	// should create file if it doesn't exist
	return ioutil.WriteFile(mysqlDbFilePath, data, 0777)
}
