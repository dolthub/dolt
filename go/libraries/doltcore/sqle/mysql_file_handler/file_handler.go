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
	"bytes"
	"context"
	"errors"
	"os"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"

	"github.com/dolthub/dolt/go/libraries/utils/file"
)

var PermsFileMode os.FileMode = 0600

type Persister struct {
	fileMutex      *sync.Mutex
	privsFilePath  string
	doltCfgDirPath string
}

var _ mysql_db.MySQLDbPersistence = &Persister{}

func NewPersister(fp string, dp string) *Persister {
	return &Persister{
		privsFilePath:  fp,
		doltCfgDirPath: dp,
		fileMutex:      &sync.Mutex{},
	}
}

func (p *Persister) Persist(ctx *sql.Context, data []byte) error {
	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()

	// Create doltcfg directory if it doesn't already exist
	if len(p.doltCfgDirPath) != 0 {
		if _, err := os.Stat(p.doltCfgDirPath); os.IsNotExist(err) {
			if err := os.Mkdir(p.doltCfgDirPath, 0777); err != nil {
				return err
			}
		}
	}

	return file.WriteFileAtomically(p.privsFilePath, bytes.NewReader(data), PermsFileMode)
}

// LoadData reads the mysql.db file, returns nil if empty or not found
func (p Persister) LoadData(context.Context) ([]byte, error) {
	// do nothing if no filepath specified
	if len(p.privsFilePath) == 0 {
		return nil, nil
	}

	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()

	// read from mysqldbFilePath, error if something other than not-exists
	buf, err := os.ReadFile(p.privsFilePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, nil
	}

	return buf, nil
}
