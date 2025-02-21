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
	"github.com/dolthub/go-mysql-server/sql"
)

var ErrStatsIssuerPaused = fmt.Errorf("stats issuer is paused")

type listenerEvent uint8

const (
	unknownEvent = listenerEvent(iota)
	leSwap
	leStop
	leGc = 4
)

func (sc *StatsController) signalListener(s listenerEvent) {
	for _, l := range sc.listeners {
		l <- s
		close(l)
	}
	sc.listeners = sc.listeners[:0]
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

func (sc *StatsController) addListener() (chan listenerEvent, error) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	if sc.activeCtxCancel == nil {
		return nil, ErrStatsIssuerPaused
	}
	l := make(chan listenerEvent, 1)
	sc.listeners = append(sc.listeners, l)
	return l, nil
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

func (sc *StatsController) waitForCond(ctx context.Context, ok, stop listenerEvent, cnt int, retry func()) (err error) {
	for cnt > 0 {
		var l chan listenerEvent
		l, err = sc.addListener()
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case e := <-l:
			if (ok & e) > 0 {
				cnt--
			} else if (stop & e) > 0 {
				return ErrStatsIssuerPaused
			}
		}
		if retry != nil {
			retry()
		}
	}
	return nil
}

func (sc *StatsController) WaitForDbSync(ctx context.Context) (err error) {
	// wait for 2 cycles because first completion is usually a stale context
	return sc.waitForCond(ctx, leSwap|leGc, leStop, 2, nil)
}

func (sc *StatsController) Gc(ctx *sql.Context) error {
	sc.doGc = true
	return sc.waitForCond(ctx, leGc, leStop, 1, func() {
		sc.statsMu.Lock()
		defer sc.statsMu.Unlock()
		sc.doGc = true
	})
}

func (sc *StatsController) Close() {
	sc.sq.Stop()
	sc.Stop()
	close(sc.closed)
	return
}
