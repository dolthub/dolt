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

package dprocedures

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var statsFuncSchema = []*sql.Column{
	{
		Name:     "message",
		Type:     gmstypes.LongText,
		Nullable: true,
	},
}

func statsFunc(fn func(ctx *sql.Context) (interface{}, error)) func(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	return func(ctx *sql.Context, args ...string) (sql.RowIter, error) {
		res, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		return rowToIter(res), nil
	}
}

// ToggableStats is a sql.StatsProvider that exposes hooks for
// observing and manipulating background database auto refresh threads.
type ToggableStats interface {
	sql.StatsProvider
	CancelRefreshThread(string)
	StartRefreshThread(*sql.Context, dsess.DoltDatabaseProvider, string, *env.DoltEnv, dsess.SqlDatabase) error
	ThreadStatus(string) string
	Prune(ctx *sql.Context) error
	Purge(ctx *sql.Context) error
	WaitForDbSync(ctx *sql.Context) error
}

type BranchStatsProvider interface {
	DropBranchDbStats(ctx *sql.Context, branch, db string, flush bool) error
}

// statsRestart tries to stop and then start a refresh thread
func statsRestart(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()
	dbName := strings.ToLower(ctx.GetCurrentDatabase())

	if afp, ok := statsPro.(ToggableStats); ok {
		pro := dSess.Provider()
		newFs, err := pro.FileSystemForDatabase(dbName)
		if err != nil {
			return nil, fmt.Errorf("failed to restart stats collection: %w", err)
		}

		dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, newFs, pro.DbFactoryUrl(), "TODO")

		sqlDb, ok := pro.BaseDatabase(ctx, dbName)
		if !ok {
			return nil, fmt.Errorf("failed to restart stats collection: database not found: %s", dbName)
		}

		afp.CancelRefreshThread(dbName)

		err = afp.StartRefreshThread(ctx, pro, dbName, dEnv, sqlDb)
		if err != nil {
			return nil, fmt.Errorf("failed to restart collection: %w", err)
		}
		return fmt.Sprintf("restarted stats collection: %s", ref.StatsRef{}.String()), nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsStatus returns the last update for a stats thread
func statsStatus(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	dbName := strings.ToLower(ctx.GetCurrentDatabase())
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		return afp.ThreadStatus(dbName), nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsStatus returns the last update for a stats thread
func statsWait(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		afp.WaitForDbSync(ctx)
		return nil, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsStop cancels a refresh thread
func statsStop(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()
	dbName := strings.ToLower(ctx.GetCurrentDatabase())

	if afp, ok := statsPro.(ToggableStats); ok {
		afp.CancelRefreshThread(dbName)
		return fmt.Sprintf("stopped thread: %s", dbName), nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsDrop deletes the stats ref
func statsDrop(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	dbName := strings.ToLower(ctx.GetCurrentDatabase())

	branch, err := dSess.GetBranch()
	if err != nil {
		return nil, fmt.Errorf("failed to drop stats: %w", err)
	}

	if afp, ok := pro.(ToggableStats); ok {
		// currently unsafe to drop stats while running refresh
		afp.CancelRefreshThread(dbName)
	}
	if bsp, ok := pro.(BranchStatsProvider); ok {
		err := bsp.DropBranchDbStats(ctx, branch, dbName, true)
		if err != nil {
			return nil, fmt.Errorf("failed to drop stats: %w", err)
		}
	}

	return fmt.Sprintf("deleted stats ref for %s", dbName), nil
}

// statsPrune replaces the current disk contents with only the currently
// tracked in memory statistics.
func statsPrune(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro, ok := dSess.StatsProvider().(ToggableStats)
	if !ok {
		return nil, fmt.Errorf("stats not persisted, cannot purge")
	}
	if err := pro.Prune(ctx); err != nil {
		return "failed to prune stats databases", err
	}
	return "pruned all stats databases", nil
}

// statsPurge removes the stats database from disk
func statsPurge(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro, ok := dSess.StatsProvider().(ToggableStats)
	if !ok {
		return nil, fmt.Errorf("stats not persisted, cannot purge")
	}
	if err := pro.Purge(ctx); err != nil {
		return "failed to purged databases", err
	}
	return "purged all database stats", nil
}
