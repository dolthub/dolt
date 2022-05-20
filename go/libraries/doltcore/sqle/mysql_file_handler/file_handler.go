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
	fileMutex.Lock()
	defer fileMutex.Unlock()

	_, err := os.Stat(fp)
	if err != nil {
		// Some strange unknown failure, okay to panic here
		if !errors.Is(err, os.ErrNotExist) {
			panic(err)
		}
	}
	privsFilePath = fp
}

// SetMySQLDbFilePath sets the file path that will be used for saving and loading MySQL Db tables.
func SetMySQLDbFilePath(fp string) {
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
	mysqlDbFilePath = fp
}

// LoadPrivileges reads the file previously set on the file path and returns the privileges and role connections. If the
// file path has not been set, returns an empty slice for both, but does not error. This is so that the logic path can
// retain the calls regardless of whether a user wants privileges to be loaded or persisted.
func LoadPrivileges() ([]*mysql_db.User, []*mysql_db.RoleEdge, error) {
	fileMutex.Lock()
	defer fileMutex.Unlock()
	if privsFilePath == "" {
		return nil, nil, nil
	}

	fileContents, err := ioutil.ReadFile(privsFilePath)
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
func LoadData() ([]byte, error) {
	fileMutex.Lock()
	defer fileMutex.Unlock()
	if mysqlDbFilePath == "" {
		return nil, nil
	}

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

	return ioutil.WriteFile(mysqlDbFilePath, data, 0777)
}
