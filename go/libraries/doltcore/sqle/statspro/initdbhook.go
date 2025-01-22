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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

func NewStatsInitDatabaseHook2(
	sc *StatsCoord,
	ctxFactory func(ctx context.Context) (*sql.Context, error),
	bThreads *sql.BackgroundThreads,
) sqle.InitDatabaseHook {
	return func(
		ctx *sql.Context,
		_ *sqle.DoltDatabaseProvider,
		name string,
		denv *env.DoltEnv,
		db dsess.SqlDatabase,
	) error {
		sqlDb, ok := db.(sqle.Database)
		if !ok {
			sc.logger.Debugf("stats initialize db failed, expected *sqle.Database, found %T", db)
			return nil
		}

		dsessDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, "main", "main/"+sqlDb.AliasedName())
		if err != nil {
			sc.logger.Debugf("stats initialize db failed, main branch not found")
		}

		sqlDb, ok = dsessDb.(sqle.Database)
		if !ok {
			sc.logger.Debugf("stats initialize db failed, expected *sqle.Database, found %T", db)
			return nil
		}

		done := sc.Add(ctx, sqlDb)

		// wait for seed job to finish, unless stats are stopped
		for {
			select {
			case <-sc.Done:
				sc.logger.Debugf("stats jobs interrupted before initialize %s complete", sqlDb.Name())
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
				return nil
			}
		}
	}
}

func NewStatsDropDatabaseHook2(sc *StatsCoord) sqle.DropDatabaseHook {
	return func(ctx *sql.Context, name string) {
		if err := sc.DropDbStats(ctx, name, false); err != nil {
			ctx.GetLogger().Debugf("failed to close stats database: %s", err)
		}

		// todo delete stats db?
	}
}
