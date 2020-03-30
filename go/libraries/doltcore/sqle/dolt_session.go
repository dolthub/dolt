// Copyright 2020 Liquidata, Inc.
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
	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
)

type dbRoot struct {
	hashStr string
	root    *doltdb.RootValue
}

// DoltSession is the sql.Session implementation used by dolt.  It is accessible through a *sql.Context instance
type DoltSession struct {
	sql.Session
	dbRoots map[string]dbRoot
}

// DefaultDoltSession creates a DoltSession object with default values
func DefaultDoltSession() *DoltSession {
	return &DoltSession{sql.NewBaseSession(), make(map[string]dbRoot)}
}

// NewSessionWithDefaultRoot creates a DoltSession object from a standard sql.Session and 0 or more Database objects.
func NewSessionWithDefaultRoots(sqlSess sql.Session, dbs ...Database) (*DoltSession, error) {
	dbRoots := make(map[string]dbRoot)
	for _, db := range dbs {
		defRoot := db.GetDefaultRoot()
		h, err := defRoot.HashOf()

		if err != nil {
			return nil, err
		}

		hashStr := h.String()

		dbRoots[db.Name()] = dbRoot{hashStr: hashStr, root: defRoot}
	}

	return &DoltSession{sqlSess, dbRoots}, nil
}

// DSessFromSess retrieves a dolt session from a standard sql.Session
func DSessFromSess(sess sql.Session) *DoltSession {
	return sess.(*DoltSession)
}
