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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/go-mysql-server/sql"
	"sync"
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
	j := 0
	for i := 0; i < len(sc.listeners); i++ {
		l := sc.listeners[i]
		if (l.e|leStop)&s > 0 {
			l.c <- s
			close(l.c)
		} else {
			sc.listeners[j] = sc.listeners[i]
			j++
		}
	}
	sc.listeners = sc.listeners[:j]
}

func (sc *StatsController) newThreadCtx(ctx context.Context) context.Context {
	sc.statsMu.Lock()
	sc.statsMu.Unlock()
	newCtx, cancel := context.WithCancel(ctx)
	if sc.activeCtxCancel != nil {
		sc.activeCtxCancel()
	}
	sc.signalListener(leStop)
	sc.activeCtxCancel = cancel
	return newCtx
}

type listenMsg struct {
	e listenerEvent
	c chan listenerEvent
}

func (sc *StatsController) addListener(e listenerEvent) (chan listenerEvent, error) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	if sc.activeCtxCancel == nil {
		return nil, ErrStatsIssuerPaused
	}
	l := listenMsg{e: e, c: make(chan listenerEvent, 1)}
	sc.listeners = append(sc.listeners, l)
	return l.c, nil
}

func (sc *StatsController) Stop() {
	// xxx: do not pause |sq|, analyze jobs still need to run
	sc.statsMu.Lock()
	sc.statsMu.Unlock()
	if sc.activeCtxCancel != nil {
		sc.activeCtxCancel()
		sc.activeCtxCancel = nil
	}
	sc.signalListener(leStop)
	return
}

func (sc *StatsController) Restart() error {
	select {
	case <-sc.closed:
		return fmt.Errorf("StatsController is closed")
	default:
	}
	sc.sq.Start()
	done := make(chan struct{})
	go func() {
		ctx := sc.newThreadCtx(context.Background())
		close(done)
		sc.runIssuer(ctx)
	}()
	// only return after latestCtx updated
	<-done
	return nil
}

func (sc *StatsController) RunQueue() {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		sc.sq.Run(context.Background())
	}()
	wg.Wait()
	return
}

func (sc *StatsController) Init(ctx context.Context, dbs []sql.Database, keepStorage bool) error {
	sc.RunQueue()
	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return err
	}
	for i, db := range dbs {
		if db, ok := db.(sqle.Database); ok { // exclude read replica dbs
			fs, err := sc.pro.FileSystemForDatabase(db.AliasedName())
			if err != nil {
				return err
			}
			if err := sc.AddFs(sqlCtx, db, fs); err != nil {
				return err
			}
			if i == 0 && !keepStorage {
				if err := sc.lockedRotateStorage(sqlCtx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (sc *StatsController) waitForCond(ctx context.Context, ok listenerEvent, cnt int) (err error) {
	for cnt > 0 {
		var l chan listenerEvent
		l, err = sc.addListener(ok)
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
	return sc.waitForCond(ctx, leSwap, 2)
}

func (sc *StatsController) WaitForFlush(ctx *sql.Context) error {
	return sc.waitForCond(ctx, leFlush, 1)
}

func (sc *StatsController) Gc(ctx *sql.Context) error {
	sc.doGc = true
	return sc.waitForCond(ctx, leGc, 1)
}

func (sc *StatsController) Close() {
	//sc.sq.Purge()
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	if sc.activeCtxCancel != nil {
		sc.activeCtxCancel()
		sc.activeCtxCancel = nil
		sc.sq.InterruptAsync(func() error {
			sc.sq.Purge()
			sc.sq.Stop()
			return nil
		})
	}
	sc.signalListener(leStop)

	close(sc.closed)
	return
}
