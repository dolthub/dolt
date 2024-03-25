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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

func NewInitDatabaseHook(
	statsProv *Provider,
	ctxFactory func(ctx context.Context) (*sql.Context, error),
	bThreads *sql.BackgroundThreads,
	orig sqle.InitDatabaseHook,
) sqle.InitDatabaseHook {
	return func(
		ctx *sql.Context,
		pro *sqle.DoltDatabaseProvider,
		name string,
		denv *env.DoltEnv,
		db dsess.SqlDatabase,
	) error {
		// We assume there is nothing on disk to read. Probably safe and also
		// would deadlock with dbProvider if we tried from reading root/session.
		if orig != nil {
			err := orig(ctx, pro, name, denv, db)
			if err != nil {
				return err
			}
		}
		
		statsDb, err := statsProv.sf.Init(ctx, db, statsProv.pro, denv.FS, env.GetCurrentUserHomeDir)
		if err != nil {
			ctx.Warn(0, err.Error())
			return nil
		}
		statsProv.mu.Lock()
		statsProv.setStatDb(strings.ToLower(db.Name()), statsDb)
		statsProv.mu.Unlock()

		ctx.GetLogger().Debugf("statistics refresh: initialize %s", name)
		return statsProv.InitAutoRefresh(ctxFactory, name, bThreads)
	}
}

func NewDropDatabaseHook(statsProv *Provider, ctxFactory func(ctx context.Context) (*sql.Context, error), orig sqle.DropDatabaseHook) sqle.DropDatabaseHook {
	return func(name string) {
		if orig != nil {
			orig(name)
		}
		ctx, err := ctxFactory(context.Background())
		if err != nil {
			return
		}
		statsProv.CancelRefreshThread(name)
		statsProv.DropDbStats(ctx, name, false)

		if db, ok := statsProv.getStatDb(name); ok {
			db.Close()
		}
	}
}
