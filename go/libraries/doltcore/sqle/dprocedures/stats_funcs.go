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
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

func statsFunc(name string) func(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	var fn func(*sql.Context) (interface{}, error)
	switch name {
	case "clear":
		fn = statsClear
	case "restart":
		fn = statsRestart
	case "stop":
		fn = statsStop
	case "status":
		fn = statsStatus
	}
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

func statsClear(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()

	err := pro.DropDbStats(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, fmt.Errorf("failed to clear stats: %w", err)
	}
	return fmt.Sprintf("deleted for for %s: %s", ctx.GetCurrentDatabase(), ref.StatsRef{}.String()), nil
}

func statsRestart(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()
	dbName := strings.ToLower(ctx.GetCurrentDatabase())

	if afp, ok := statsPro.(AutoRefreshStatsProvider); ok {
		err := statsPro.DropDbStats(ctx, dbName)
		if err != nil {
			return nil, fmt.Errorf("failed to stop stats collection: %w", err)
		}

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
	return nil, nil
}

func statsStatus(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	dbName := strings.ToLower(ctx.GetCurrentDatabase())
	pro := dSess.StatsProvider()
	if afp, ok := pro.(AutoRefreshStatsProvider); ok {
		return afp.ThreadStatus(dbName), nil
	}
	return nil, fmt.Errorf("provider does not implement AutoRefreshStatsProvider")
}

func statsStop(ctx *sql.Context) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	dbName := strings.ToLower(ctx.GetCurrentDatabase())

	err := pro.DropDbStats(ctx, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to clear stats: %w", err)
	}
	return fmt.Sprintf("deleted for for %s: %s", dbName, ref.StatsRef{}.String()), nil
}
