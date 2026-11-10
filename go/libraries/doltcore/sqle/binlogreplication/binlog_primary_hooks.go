// Copyright 2024 Dolthub, Inc.
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

package binlogreplication

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// NewBinlogInitDatabaseHook returns an InitDatabaseHook function that records the database creation in the binlog events.
func NewBinlogInitDatabaseHook(_ context.Context, listeners []doltdb.DatabaseUpdateListener) func(ctx *sql.Context, pro *sqle.DoltDatabaseProvider, name string, denv *env.DoltEnv, db dsess.SqlDatabase) error {
	return func(
		ctx *sql.Context,
		pro *sqle.DoltDatabaseProvider,
		name string,
		denv *env.DoltEnv,
		db dsess.SqlDatabase,
	) error {
		for _, listener := range listeners {
			err := listener.DatabaseCreated(ctx, name)
			if err != nil {
				logrus.Errorf("error notifying working root listener of dropped database: %s", err.Error())
				return err
			}

			// After creating the database, try to replicate any existing data.
			// This is only needed when dolt_undrop() has been used to restore a dropped database.
			err = replicateExistingData(ctx, denv.DoltDB(ctx), BinlogBranch, listener, name)
			if err != nil {
				logrus.Errorf("error replicating data from newly created database: %s", err.Error())
				return err
			}
		}
		return nil
	}
}

// replicateExistingData replicates any existing data from |ddb| to the specified |listener| after the database is
// created. A newly created database can only have existing data in the case where it has been undropped via
// dolt_undrop().
func replicateExistingData(ctx *sql.Context, ddb *doltdb.DoltDB, branchName string, listener doltdb.DatabaseUpdateListener, databaseName string) error {
	roots, err := ddb.ResolveBranchRoots(ctx, ref.NewBranchRef(branchName))
	if err == doltdb.ErrBranchNotFound {
		// If the branch doesn't exist, just return
		return nil
	} else if err != nil {
		return err
	}

	emptyRoot, err := doltdb.EmptyRootValue(ctx, roots.Working.VRW(), roots.Working.NodeStore())
	if err != nil {
		return err
	}

	return listener.WorkingRootUpdated(ctx, databaseName, branchName, emptyRoot, roots.Working)
}

// NewBinlogDropDatabaseHook returns a new DropDatabaseHook function that records a database drop in the binlog events.
func NewBinlogDropDatabaseHook(_ context.Context, listeners []doltdb.DatabaseUpdateListener) func(ctx *sql.Context, name string) {
	return func(ctx *sql.Context, name string) {
		for _, listener := range listeners {
			err := listener.DatabaseDropped(ctx, name)
			if err != nil {
				logrus.Errorf("error notifying working root listener of dropped database: %s", err.Error())
			}
		}
	}
}
