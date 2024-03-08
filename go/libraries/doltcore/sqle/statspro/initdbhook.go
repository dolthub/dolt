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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func NewInitDatabaseHook(statsProv *Provider, ctxFactory func(ctx context.Context) (*sql.Context, error), bThreads *sql.BackgroundThreads, orig sqle.InitDatabaseHook) sqle.InitDatabaseHook {
	return func(ctx *sql.Context, pro *sqle.DoltDatabaseProvider, name string, denv *env.DoltEnv, db dsess.SqlDatabase) error {
		if orig != nil {
			err := orig(ctx, pro, name, denv, db)
			if err != nil {
				return err
			}
		}

		dSess := dsess.DSessFromSess(ctx.Session)
		var branches []string
		if _, bs, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsBranches); bs == "" {
			defaultBranch, _ := dSess.GetBranch()
			if defaultBranch != "" {
				branches = append(branches, defaultBranch)
			}
		} else {
			for _, branch := range strings.Split(bs.(string), ",") {
				branches = append(branches, strings.TrimSpace(branch))
			}
		}

		if branches == nil {
			branches = []string{pro.DefaultBranch()}
		}

		if err := statsProv.Load(ctx, denv.FS, db, branches); err != nil {
			return err
		}

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
	}
}
