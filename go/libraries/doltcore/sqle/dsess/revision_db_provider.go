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

package dsess

import (
	"github.com/dolthub/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
)

// ErrRevisionDbDoesNotExist is thrown when a RevisionDatabaseProvider cannot find a specified revision database.
var ErrRevisionDbDoesNotExist = errors.NewKind("revision database not found: '%s'")

// RevisionDatabaseProvider provides revision databases.
// In Dolt, commits and branches can be accessed as discrete databases
// using a Dolt-specific syntax: `my_database/my_branch`. Revision databases
// corresponding to historical commits in the repository will be read-only
// databases. Revision databases for branches will be read/write.
type RevisionDatabaseProvider interface {
	// RevisionDbState provides the InitialDbState for a revision database.
	RevisionDbState(ctx *sql.Context, revDB string) (InitialDbState, error)
	// DropRevisionDb removes the specified revision database from the databases this provider is tracking.
	DropRevisionDb(ctx *sql.Context, revDB string) error
}

func EmptyDatabaseProvider() RevisionDatabaseProvider {
	return emptyRevisionDatabaseProvider{}
}

type emptyRevisionDatabaseProvider struct{}

func (e emptyRevisionDatabaseProvider) DropRevisionDb(ctx *sql.Context, revDB string) error {
	return nil
}

func (e emptyRevisionDatabaseProvider) RevisionDbState(_ *sql.Context, revDB string) (InitialDbState, error) {
	return InitialDbState{}, sql.ErrDatabaseNotFound.New(revDB)
}
