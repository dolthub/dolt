// Copyright 2025 Dolthub, Inc.
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
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var ErrStatsIssuerPaused = fmt.Errorf("stats issuer is paused")

type listenerEvent uint16

const (
	leUnknown               = listenerEvent(iota)
	leSwap    listenerEvent = 1 << 0
	leStop    listenerEvent = 1 << 1
	leGc      listenerEvent = 1 << 2
	leFlush   listenerEvent = 1 << 3
)

func (sc *StatsController) signalListener(s listenerEvent) {
	keep := 0
	for i, l := range sc.listeners {
		if (l.target|leStop)&s > 0 {
			l.c <- s
			close(l.c)
		} else {
			sc.listeners[keep] = sc.listeners[i]
			keep++
		}
	}
	sc.listeners = sc.listeners[:keep]
}

func (sc *StatsController) newThreadCtx(ctx context.Context) context.Context {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	newCtx, cancel := context.WithCancel(ctx)
	if sc.activeCtxCancel != nil {
		sc.activeCtxCancel()
	}
	sc.signalListener(leStop)
	sc.activeCtxCancel = cancel
	return newCtx
}

type listener struct {
	target listenerEvent
	c      chan listenerEvent
}

func (sc *StatsController) addListener(e listenerEvent) (chan listenerEvent, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.activeCtxCancel == nil {
		return nil, ErrStatsIssuerPaused
	}
	l := listener{target: e, c: make(chan listenerEvent, 1)}
	sc.listeners = append(sc.listeners, l)
	return l.c, nil
}

func (sc *StatsController) Stop() {
	// xxx: do not pause |sq|, analyze jobs still need to run
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.activeCtxCancel != nil {
		sc.activeCtxCancel()
		sc.activeCtxCancel = nil
	}
	sc.signalListener(leStop)
	return
}

// RefreshFromSysVars reads the environment variables and updates controller
// parameters. If the queue is not started this will hang.
func (sc *StatsController) RefreshFromSysVars() {
	_, memOnly, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsMemoryOnly)
	sc.SetMemOnly(memOnly.(int8) == 1)

	_, gcEnabled, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsGCEnabled)
	sc.SetEnableGc(gcEnabled.(int8) == 1)

	typ, jobI, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsJobInterval)
	_, gcI, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsGCInterval)

	jobInterval, _, _ := typ.GetType().Convert(jobI)
	gcInterval, _, _ := typ.GetType().Convert(gcI)

	sc.SetTimers(
		jobInterval.(int64)*int64(time.Millisecond),
		gcInterval.(int64)*int64(time.Millisecond),
	)
}

func (sc *StatsController) Restart() error {
	select {
	case <-sc.closed:
		return fmt.Errorf("StatsController is closed")
	default:
	}

	sc.sq.Start()
	sc.RefreshFromSysVars()

	done := make(chan struct{})
	if err := sc.bgThreads.Add("stats_worker", func(ctx context.Context) {
		ctx = sc.newThreadCtx(ctx)
		close(done)
		err := sc.runWorker(ctx)
		if err != nil {
			sc.logger.Errorf("stats stopped: %s", err.Error())
		}
	}); err != nil {
		return err
	}
	// only return after latestCtx updated
	<-done
	return nil
}

func (sc *StatsController) RunQueue() {
	if err := sc.bgThreads.Add("stats_scheduler", sc.sq.Run); err != nil {
		sc.descError("start scheduler", err)
	}
	// block on queue starting
	sc.sq.DoSync(context.Background(), func() error { return nil })
	return
}

// Init should only be called once
func (sc *StatsController) Init(ctx context.Context, pro *sqle.DoltDatabaseProvider, ctxGen ctxFactory, dbs []sql.Database) error {
	sc.pro = pro
	sc.ctxGen = ctxGen

	sc.RunQueue()
	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return err
	}
	defer sql.SessionEnd(sqlCtx.Session)
	sql.SessionCommandBegin(sqlCtx.Session)
	defer sql.SessionCommandEnd(sqlCtx.Session)

	for i, db := range dbs {
		if db, ok := db.(sqle.Database); ok { // exclude read replica dbs
			fs, err := sc.pro.FileSystemForDatabase(db.AliasedName())
			if err != nil {
				return err
			}
			if err := sc.AddFs(sqlCtx, db, fs, false); err != nil {
				return err
			}
			if i > 0 || sc.memOnly {
				continue
			}
			// attempt to access previously written stats
			statsFs, err := fs.WithWorkingDir(dbfactory.DoltStatsDir)
			if err != nil {
				return err
			}

			exists, isDir := statsFs.Exists("")
			if exists && isDir {
				newKv, err := sc.initStorage(ctx, fs)
				if err == nil {
					sc.kv = newKv
					sc.statsBackingDb = fs
					continue
				} else {
					path, _ := statsFs.Abs("")
					sc.descError("failed to reboot stats from: "+path, err)
				}
			}

			// otherwise wipe and create new stats dir
			if err := sc.lockedRotateStorage(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sc *StatsController) waitForSignal(ctx context.Context, signal listenerEvent, cnt int) (err error) {
	for cnt > 0 {
		var l chan listenerEvent
		l, err = sc.addListener(signal)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-l:
			cnt--
		}
	}
	return nil
}

func (sc *StatsController) WaitForSync(ctx context.Context) (err error) {
	// wait for 2 cycles because first completion is usually a stale context
	return sc.waitForSignal(ctx, leSwap, 2)
}

func (sc *StatsController) WaitForFlush(ctx *sql.Context) error {
	sc.mu.Lock()
	memOnly := sc.memOnly
	sc.mu.Unlock()
	if memOnly {
		return fmt.Errorf("memory only statistics will not flush")
	}
	return sc.waitForSignal(ctx, leFlush, 1)
}

func (sc *StatsController) Gc(ctx *sql.Context) error {
	sc.setDoGc(true)
	return sc.waitForSignal(ctx, leGc, 1)
}

func (sc *StatsController) Close() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.activeCtxCancel != nil {
		sc.activeCtxCancel()
		sc.activeCtxCancel = nil
		sc.sq.InterruptAsync(func() error {
			return sc.sq.Stop()
		})
	}
	sc.signalListener(leStop)

	close(sc.closed)
	return
}
