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
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var statsFuncSchema = []*sql.Column{
	{
		Name:     "message",
		Type:     gmstypes.LongText,
		Nullable: true,
	},
}

const OkResult = "Ok"

func statsFunc(fn func(ctx *sql.Context, args ...string) (interface{}, error)) func(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	return func(ctx *sql.Context, args ...string) (iter sql.RowIter, err error) {
		//defer func() {
		//	if r := recover(); r != nil {
		//		err = fmt.Errorf("stats function unexpectedly panicked: %s", r)
		//	}
		//}()
		res, err := fn(ctx, args...)
		if err != nil {
			return nil, err
		}
		return rowToIter(res), nil
	}
}

type StatsInfo struct {
	DbCnt             int    `json:"dbCnt"`
	Active            bool   `json:"active"`
	StorageBucketCnt  int    `json:"storageBucketCnt"`
	CachedBucketCnt   int    `json:"cachedBucketCnt"`
	CachedBoundCnt    int    `json:"cachedBoundCnt"`
	CachedTemplateCnt int    `json:"cachedTemplateCnt"`
	StatCnt           int    `json:"statCnt"`
	GcCnt             int    `json:"gcCnt,omitempty"`
	GenCnt            int    `json:"genCnt,omitempty"`
	Backing           string `json:"backing"`
}

func (si StatsInfo) ToJson(short bool) string {
	if short {
		si.GcCnt = 0
		si.GenCnt = 0
	}
	jsonData, err := json.Marshal(si)
	if err != nil {
		return ""
	}
	return string(jsonData)
}

// ToggableStats is a sql.StatsProvider that exposes hooks for
// observing and manipulating background database auto refresh threads.
type ToggableStats interface {
	sql.StatsProvider
	//FlushQueue(ctx context.Context) error
	Restart() error
	Stop()
	Info(ctx context.Context) (StatsInfo, error)
	Purge(ctx *sql.Context) error
	WaitForSync(ctx context.Context) error
	Gc(ctx *sql.Context) error
	WaitForFlush(ctx *sql.Context) error
	CollectOnce(ctx context.Context) (string, error)
	SetTimers(int64, int64)
}

type BranchStatsProvider interface {
	DropBranchDbStats(ctx *sql.Context, branch, db string, flush bool) error
}

// statsRestart flushes the current job queue and re-inits all
// statistic databases.
func statsRestart(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()

	if afp, ok := statsPro.(ToggableStats); ok {
		if err := afp.Restart(); err != nil {
			return nil, err
		}

		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsInfo returns the last update for a stats thread
func statsInfo(ctx *sql.Context, args ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		var short bool
		if len(args) > 0 && (args[0] == "-s" || args[0] == "--short") {
			short = true
		}
		info, err := afp.Info(ctx)
		if err != nil {
			return nil, err
		}
		return info.ToJson(short), nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsWait blocks until the job queue executes two full loops
// of instructions, which will (1) pick up and (2) commit new
// sets of index-bucket dependencies.
func statsSync(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		if err := afp.WaitForSync(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

func statsOnce(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		str, err := afp.CollectOnce(ctx)
		if err != nil {
			return nil, err
		}
		return str, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsWait blocks until the job queue executes two full loops
// of instructions, which will (1) pick up and (2) commit new
// sets of index-bucket dependencies.
func statsFlush(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		if err := afp.WaitForFlush(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsGc rewrites the cache to only include objects reachable
// by the current root value.
func statsGc(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		if err := afp.Gc(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsStop flushes the job queue and leaves the stats provider
// in a paused state.
func statsStop(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()

	if afp, ok := statsPro.(ToggableStats); ok {
		afp.Stop()
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsPurge flushes the job queue, deletes the current caches
// and storage targets, re-initializes the tracked database
// states, and returns with stats collection paused.
func statsPurge(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro, ok := dSess.StatsProvider().(ToggableStats)
	if !ok {
		return nil, fmt.Errorf("stats not persisted, cannot purge")
	}

	pro.Stop()

	if err := pro.Purge(ctx); err != nil {
		return "failed to purge stats", err
	}

	return OkResult, nil
}

// statsTimers updates the stats timers, which go into effect after the next restart.
func statsTimers(ctx *sql.Context, args ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()

	if len(args) != 2 {
		return nil, fmt.Errorf("expected timer arguments (ns): (job, gc)")
	}
	job, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("interval timer must be positive intergers")
	}
	gc, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("interval timer must be positive intergers")
	}

	if afp, ok := statsPro.(ToggableStats); ok {
		afp.SetTimers(job, gc)
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}
