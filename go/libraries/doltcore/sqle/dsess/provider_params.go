// Copyright 2026 Dolthub, Inc.
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
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

// RemoteDBForProvider is a DoltDatabaseProvider that opens the remotes of a database as that database opens them.
type RemoteDBForProvider interface {
	GetRemoteDBFor(ctx context.Context, local *doltdb.DoltDB, r env.Remote) (*doltdb.DoltDB, error)
}

// GetRemoteDBFor returns the remote database |r| of |local|, opened by |p|.
func GetRemoteDBFor(ctx context.Context, p DoltDatabaseProvider, local *doltdb.DoltDB, r env.Remote) (*doltdb.DoltDB, error) {
	if rp, ok := p.(RemoteDBForProvider); ok {
		return rp.GetRemoteDBFor(ctx, local, r)
	}
	return p.GetRemoteDB(ctx, local.Format(), r)
}

// ParamsDatabaseCreator is a DoltDatabaseProvider that creates databases with dbfactory params.
type ParamsDatabaseCreator interface {
	CreateDatabaseWithParams(ctx *sql.Context, name string, params map[string]string) error
}

// CreateDatabaseWithParams creates the database |name| in |p| with the dbfactory |params|, if there are any.
func CreateDatabaseWithParams(ctx *sql.Context, p DoltDatabaseProvider, name string, params map[string]string) error {
	if len(params) == 0 {
		return p.CreateDatabase(ctx, name)
	}
	if c, ok := p.(ParamsDatabaseCreator); ok {
		return c.CreateDatabaseWithParams(ctx, name, params)
	}
	return fmt.Errorf("cannot create database %s: %T does not take database params", name, p)
}
