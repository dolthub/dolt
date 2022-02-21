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
	"io/ioutil"
	"os"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
)

var (
	filePath  string
	fileMutex = &sync.Mutex{}
)

// privDataJson is used to marshal/unmarshal the privilege data to/from JSON.
type privDataJson struct {
	Users []*grant_tables.User
	Roles []*grant_tables.RoleEdge
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
func LoadPrivileges() ([]*grant_tables.User, []*grant_tables.RoleEdge, error) {
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

var _ grant_tables.PersistCallback = SavePrivileges

// SavePrivileges implements the interface grant_tables.PersistCallback. This is used to save privileges to disk. If the
// file path has not been previously set, this returns without error. This is so that the logic path can retain the
// calls regardless of whether a user wants privileges to be loaded or persisted.
func SavePrivileges(ctx *sql.Context, users []*grant_tables.User, roles []*grant_tables.RoleEdge) error {
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
