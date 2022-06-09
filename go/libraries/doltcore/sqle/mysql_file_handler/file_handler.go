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
	"errors"
	"io/ioutil"
	"os"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

type Persister struct {
	privsFilePath string
	fileMutex     *sync.Mutex
}

var _ mysql_db.MySQLDbPersistence = &Persister{}

func NewPersister(fp string) *Persister {
	// Create file if it does not exist, panic if something goes wrong
	if len(fp) > 0 {
		_, err := os.Stat(fp)
		if err != nil && errors.Is(err, os.ErrNotExist) {
			err = ioutil.WriteFile(fp, []byte{}, 0644)
		}
		if err != nil {
			panic(err)
		}
	}

	return &Persister{
		privsFilePath: fp,
		fileMutex:     &sync.Mutex{},
	}
}

func (p *Persister) CanPersist() bool {
	return len(p.privsFilePath) != 0
}

func (p *Persister) Persist(ctx *sql.Context, data []byte) error {
	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()

	if len(p.privsFilePath) == 0 {
		return errors.New("no privilege file specified, to persist users/grants run with --privilege-file=<file_path>")
	}

	return ioutil.WriteFile(p.privsFilePath, data, 0777)
}

// SetPrivilegeFilePath sets the file path that will be used for loading privileges.
// TODO: this is probably not needed
func (p Persister) SetPrivilegeFilePath(fp string) {
	// do nothing for empty file path
	if len(fp) == 0 {
		return
	}

	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()

	// Create file if it does not exist, panic if something goes wrong
	_, err := os.Stat(fp)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		err = ioutil.WriteFile(fp, []byte{}, 0644)
	}
	if err != nil {
		panic(err)
	}
	p.privsFilePath = fp
}

// LoadData reads the mysql.db file, returns nil if empty or not found
func (p Persister) LoadData() ([]byte, error) {
	// do nothing if no filepath specified
	if len(p.privsFilePath) == 0 {
		return nil, nil
	}

	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()

	// read from mysqldbFilePath, error if something other than not-exists
	buf, err := ioutil.ReadFile(p.privsFilePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, nil
	}

	return buf, nil
}
