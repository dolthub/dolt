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

func (sc *StatsCoord) newThreadCtx(ctx context.Context) context.Context {
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

func (sc *StatsCoord) addListener() (chan listenerEvent, error) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	if sc.activeCtxCancel == nil {
		return nil, ErrStatsIssuerPaused
	}
	l := make(chan listenerEvent, 1)
	sc.listeners = append(sc.listeners, l)
	return l, nil
}

// Stop stops the issuer thread
func (sc *StatsCoord) Stop() {
	sc.statsMu.Lock()
	sc.statsMu.Unlock()
	sc.sq.Pause()
	if sc.activeCtxCancel != nil {
		sc.activeCtxCancel()
		sc.activeCtxCancel = nil
	}
	sc.signalListener(leStop)
	return
}

// Restart continues the queue and blocks until sender is running
func (sc *StatsCoord) Restart() error {
	select {
	case <-sc.closed:
		return fmt.Errorf("StatsCoord is closed")
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

func (sc *StatsCoord) waitForCond(ctx context.Context, ok, stop listenerEvent, cnt int) (err error) {
	for cnt > 0 {
		// the first cycle is usually an older context
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
		return nil
	}
	return nil
}

func (sc *StatsCoord) WaitForDbSync(ctx context.Context) (err error) {
	return sc.waitForCond(ctx, leSwapGc|leGc, leStop, 2)
}

func (sc *StatsCoord) Gc(ctx *sql.Context) error {
	sc.doGc = true
	return sc.waitForCond(ctx, leGc, leStop, 1)
}

func (sc *StatsCoord) Close() {
	sc.sq.Stop()
	sc.Stop()
	close(sc.closed)
	return
}
