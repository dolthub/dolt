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

package statspro

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

func NewStatsInitDatabaseHook(
	statsProv *Provider,
	ctxFactory func(ctx context.Context) (*sql.Context, error),
	bThreads *sql.BackgroundThreads,
) sqle.InitDatabaseHook {
	return func(
		ctx *sql.Context,
		pro *sqle.DoltDatabaseProvider,
		name string,
		denv *env.DoltEnv,
		db dsess.SqlDatabase,
	) error {
		dbName := strings.ToLower(db.Name())
		if statsDb, ok := statsProv.getStatDb(dbName); !ok {
			statsDb, err := statsProv.sf.Init(ctx, db, statsProv.pro, denv.FS, env.GetCurrentUserHomeDir)
			if err != nil {
				ctx.GetLogger().Debugf("statistics load error: %s", err.Error())
				return nil
			}
			statsProv.setStatDb(dbName, statsDb)
		} else {
			dSess := dsess.DSessFromSess(ctx.Session)
			for _, br := range statsDb.Branches() {
				branchQDbName := BranchQualifiedDatabase(dbName, br)
				sqlDb, err := dSess.Provider().Database(ctx, branchQDbName)
				if err != nil {
					ctx.GetLogger().Logger.Errorf("branch not found: %s", br)
					continue
				}
				branchQDb, ok := sqlDb.(dsess.SqlDatabase)
				if !ok {
					return fmt.Errorf("branch/database not found: %s", branchQDbName)
				}

				if ok, err := statsDb.SchemaChange(ctx, br, branchQDb); err != nil {
					return err
				} else if ok {
					if err := statsDb.DeleteBranchStats(ctx, br, true); err != nil {
						return err
					}
				}
			}
			ctx.GetLogger().Debugf("statistics init error: preexisting stats db: %s", dbName)
		}
		ctx.GetLogger().Debugf("statistics refresh: initialize %s", name)
		return statsProv.InitAutoRefresh(ctxFactory, name, bThreads)
	}
}

func NewStatsDropDatabaseHook(statsProv *Provider) sqle.DropDatabaseHook {
	return func(ctx *sql.Context, name string) {
		statsProv.CancelRefreshThread(name)
		if err := statsProv.DropDbStats(ctx, name, false); err != nil {
			ctx.GetLogger().Debugf("failed to close stats database: %s", err)
		}

		if db, ok := statsProv.getStatDb(name); ok {
			if err := db.Close(); err != nil {
				ctx.GetLogger().Debugf("failed to close stats database: %s", err)
			}
			delete(statsProv.statDbs, name)
		}
	}
}
