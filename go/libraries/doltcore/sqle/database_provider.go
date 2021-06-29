// Copyright 2021 Dolthub, Inc.
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

package sqle

import (
	"github.com/dolthub/go-mysql-server/sql"
)

type DoltDatabaseProvider struct {
	databases map[string]sql.Database
}

var _ sql.DatabaseProvider = DoltDatabaseProvider{}

func NewDoltDatabaseProvider(databases ...Database) sql.MutableDatabaseProvider {
	dbs := make(map[string]sql.Database, len(databases))
	for _, db := range databases {
		dbs[db.Name()] = db
	}

	return DoltDatabaseProvider{databases: dbs}
}

func (p DoltDatabaseProvider) Database(name string) (db sql.Database, err error) {
	var ok bool
	if db, ok = p.databases[name]; ok {
		return db, nil
	}

	// TODO: dynamically provide databases for refs
	// eg: "USE mydatabase/mybranch;"

	return nil, sql.ErrDatabaseNotFound.New(name)
}

func (p DoltDatabaseProvider) HasDatabase(name string) bool {
	_, err := p.Database(name)
	return err == nil
}

func (p DoltDatabaseProvider) AllDatabases() (all []sql.Database) {
	i := 0
	all = make([]sql.Database, len(p.databases))
	for _, db := range p.databases {
		all[i] = db
		i++
	}
	return
}

func (p DoltDatabaseProvider) AddDatabase(db sql.Database) {
	p.databases[db.Name()] = db
}

func (p DoltDatabaseProvider) DropDatabase(name string) {
	delete(p.databases, name)
}
