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

// AutoRefreshStatsProvider is a sql.StatsProvider that exposes hooks for
// observing and manipulating background database auto refresh threads.
type AutoRefreshStatsProvider interface {
	sql.StatsProvider
	CancelRefreshThread(string)
	StartRefreshThread(*sql.Context, dsess.DoltDatabaseProvider, string, *env.DoltEnv) error
	ThreadStatus(string) string
}

// statsRestart tries to stop and then start a refresh thread
func statsRestart(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()
	dbName := strings.ToLower(ctx.GetCurrentDatabase())

	if afp, ok := statsPro.(AutoRefreshStatsProvider); ok {
		pro := dSess.Provider()
		newFs, err := pro.FileSystem().WithWorkingDir(dbName)
		if err != nil {
			return nil, fmt.Errorf("failed to restart stats collection: %w", err)
		}

		dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, newFs, pro.DbFactoryUrl(), "TODO")

		afp.CancelRefreshThread(dbName)

		err = afp.StartRefreshThread(ctx, pro, dbName, dEnv)
		if err != nil {
			return nil, fmt.Errorf("failed to restart collection: %w", err)
		}
		return fmt.Sprintf("restarted stats collection: %s", ref.StatsRef{}.String()), nil
	}
	return nil, fmt.Errorf("provider does not implement AutoRefreshStatsProvider")
}

// statsStatus returns the last update for a stats thread
func statsStatus(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	dbName := strings.ToLower(ctx.GetCurrentDatabase())
	pro := dSess.StatsProvider()
	if afp, ok := pro.(AutoRefreshStatsProvider); ok {
		return afp.ThreadStatus(dbName), nil
	}
	return nil, fmt.Errorf("provider does not implement AutoRefreshStatsProvider")
}

// statsStop cancels a refresh thread
func statsStop(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()
	dbName := strings.ToLower(ctx.GetCurrentDatabase())

	if afp, ok := statsPro.(AutoRefreshStatsProvider); ok {
		afp.CancelRefreshThread(dbName)
		return fmt.Sprintf("stopped thread: %s", dbName), nil
	}
	return nil, fmt.Errorf("provider does not implement AutoRefreshStatsProvider")
}

// statsDrop deletes the stats ref
func statsDrop(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	dbName := strings.ToLower(ctx.GetCurrentDatabase())

	if afp, ok := pro.(AutoRefreshStatsProvider); ok {
		// currently unsafe to drop stats while running refresh
		afp.CancelRefreshThread(dbName)
	}
	err := pro.DropDbStats(ctx, dbName, true)
	if err != nil {
		return nil, fmt.Errorf("failed to drop stats: %w", err)
	}
	return fmt.Sprintf("deleted stats ref for %s", dbName), nil
}
