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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	types2 "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

func NewInitDatabaseHook(statsProv *Provider, ctxFactory func(ctx context.Context) (*sql.Context, error), bThreads *sql.BackgroundThreads, orig sqle.InitDatabaseHook) sqle.InitDatabaseHook {
	return func(ctx *sql.Context, pro *sqle.DoltDatabaseProvider, name string, denv *env.DoltEnv) error {
		if orig != nil {
			err := orig(ctx, pro, name, denv)
			if err != nil {
				return err
			}
		}
		_, threshold, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshThreshold)
		_, interval, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshInterval)
		interval64, _, _ := types2.Int64.Convert(interval)
		intervalSec := time.Second * time.Duration(interval64.(int64))
		thresholdf64 := threshold.(float64)
		return statsProv.InitAutoRefresh(ctxFactory, name, bThreads, intervalSec, thresholdf64)
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
