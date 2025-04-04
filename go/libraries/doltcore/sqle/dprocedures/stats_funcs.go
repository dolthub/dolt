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
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("stats function unexpectedly panicked: %s", r)
			}
		}()
		res, err := fn(ctx, args...)
		if err != nil {
			return nil, err
		}
		return rowToIter(res), nil
	}
}

// StatsInfo gives a summary of the current coordinator stats.
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

// ToJson returns stats info as a json string. Use the |short|
// flag to exclude cycle counters.
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

// ExtendedStatsProvider is a sql.StatsProvider that exposes hooks for
// observing and manipulating background database auto refresh threads.
type ExtendedStatsProvider interface {
	sql.StatsProvider
	// Restart starts a new stats thread, finalizes any active thread
	Restart(ctx *sql.Context) error
	// Stop finalizes stats thread if active
	Stop()
	// Info returns summary statistics about the current coordinator state
	Info(ctx context.Context) (StatsInfo, error)
	// Purge wipes the memory and storage state, and pauses stats collection
	Purge(ctx *sql.Context) error
	// WaitForSync blocks until the stats state includes changes
	// from the current session
	WaitForSync(ctx context.Context) error
	// Gc forces the next stats cycle to perform a GC. Block until
	// the GC lands.
	Gc(ctx *sql.Context) error
	// WaitForFlush blocks until the next cycle finishes and flushes
	// buckets to disk.
	WaitForFlush(ctx *sql.Context) error
	// CollectOnce performs a stats update in-thread. This will contend
	// with background collection and most useful in a non-server context.
	CollectOnce(ctx context.Context) (string, error)
	// SetTimers is an access point for editing the statistics
	// delay timer. This will block if the scheduler is not running.
	SetTimers(int64, int64)
}

type BranchStatsProvider interface {
	DropBranchDbStats(ctx *sql.Context, branch, db string, flush bool) error
}

// statsRestart cancels any ongoing update thread and starts a new worker
func statsRestart(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()

	if afp, ok := statsPro.(ExtendedStatsProvider); ok {
		if err := afp.Restart(ctx); err != nil {
			return nil, err
		}

		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ExtendedStatsProvider")
}

// statsInfo returns a coordinator state summary
func statsInfo(ctx *sql.Context, args ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ExtendedStatsProvider); ok {
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
	return nil, fmt.Errorf("provider does not implement ExtendedStatsProvider")
}

// statsWait blocks until the stats worker executes two full loops
// of instructions. The second loop will include the most recent
// committed session as of this function's execution.
func statsWait(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ExtendedStatsProvider); ok {
		if err := afp.WaitForSync(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ExtendedStatsProvider")
}

// statsOnce runs a one-off worker update. This is mostly used for
// testing and grabbing statistics while in the shell. Servers
// should use `dolt_stats_wait` to avoid contending with the
// background thread.
func statsOnce(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ExtendedStatsProvider); ok {
		str, err := afp.CollectOnce(ctx)
		if err != nil {
			return nil, err
		}
		return str, nil
	}
	return nil, fmt.Errorf("provider does not implement ExtendedStatsProvider")
}

// statsFlush waits for the next stats flush to storage.
func statsFlush(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ExtendedStatsProvider); ok {
		if err := afp.WaitForFlush(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ExtendedStatsProvider")
}

// statsGc sets the |doGc| flag and waits until a worker
// performs an update/GC.
func statsGc(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ExtendedStatsProvider); ok {
		if err := afp.Gc(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ExtendedStatsProvider")
}

// statsStop flushes the job queue and leaves the stats provider
// in a paused state.
func statsStop(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()

	if afp, ok := statsPro.(ExtendedStatsProvider); ok {
		afp.Stop()
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ExtendedStatsProvider")
}

// statsPurge flushes the job queue, deletes the current caches
// and storage targets, re-initializes the tracked database
// states, and returns with stats collection paused.
func statsPurge(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro, ok := dSess.StatsProvider().(ExtendedStatsProvider)
	if !ok {
		return nil, fmt.Errorf("stats not persisted, cannot purge")
	}

	pro.Stop()

	if err := pro.Purge(ctx); err != nil {
		return "failed to purge stats", err
	}

	return OkResult, nil
}

// statsTimers updates the stats timers, which go into effect immediately.
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

	if afp, ok := statsPro.(ExtendedStatsProvider); ok {
		afp.SetTimers(job, gc)
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ExtendedStatsProvider")
}
