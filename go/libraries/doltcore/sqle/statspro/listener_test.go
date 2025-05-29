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
	"sync"
	"testing"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestListening(t *testing.T) {
	bthreads := sql.NewBackgroundThreads()
	ctx := sql.NewEmptyContext()
	defer bthreads.Shutdown()
	t.Run("ClosedDoesNotStart", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		sc.Close()
		require.Error(t, sc.Restart(ctx))
		require.Nil(t, sc.activeCtxCancel)
	})
	t.Run("IsStoppable", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		eg := errgroup.Group{}
		ctx, _, doneCh := sc.newThreadCtx(context.Background())
		eg.Go(func() error {
			defer close(doneCh)
			return sc.runWorker(ctx)
		})

		require.NotNil(t, sc.activeCtxCancel)

		l, err := sc.addListener(leSwap)
		require.NoError(t, err)
		<-l
		select {
		case <-ctx.Done():
			t.Fatal("expected latest thread ctx to be active")
		default:
		}
		<-sc.Stop()
		<-ctx.Done()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	})
	t.Run("StopsAreIdempotent", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		eg := errgroup.Group{}
		ctx, _, doneCh := sc.newThreadCtx(context.Background())
		eg.Go(func() error {
			defer close(doneCh)
			return sc.runWorker(ctx)
		})

		<-sc.Stop()
		<-sc.Stop()
		<-sc.Stop()
		<-sc.Stop()
		<-ctx.Done()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	})
	t.Run("IsRestartable", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		eg := errgroup.Group{}
		ctx1, _, doneCh1 := sc.newThreadCtx(context.Background())
		eg.Go(func() error {
			defer close(doneCh1)
			return sc.runWorker(ctx1)
		})

		ctx2, _, doneCh2 := sc.newThreadCtx(context.Background())
		eg.Go(func() error {
			defer close(doneCh2)
			return sc.runWorker(ctx2)
		})

		ctx3, _, doneCh3 := sc.newThreadCtx(context.Background())
		eg.Go(func() error {
			defer close(doneCh3)
			return sc.runWorker(ctx3)
		})

		<-ctx1.Done()
		<-ctx2.Done()
		<-sc.Stop()
		<-ctx3.Done()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	})
	t.Run("ConcurrentStartStopsAreOk", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range 20 {
				require.NoError(t, sc.Restart(ctx))
				l, err := sc.addListener(leSwap)
				if err != nil {
					require.ErrorIs(t, err, ErrStatsIssuerPaused)
					continue
				}
				select {
				case <-l:
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range 20 {
				sc.Stop()
				l, err := sc.addListener(leSwap)
				if err != nil {
					require.ErrorIs(t, err, ErrStatsIssuerPaused)
					continue
				}
				select {
				case <-l:
				case <-time.Tick(10 * time.Millisecond):
					print()
				}
			}
		}()
		wg.Wait()
	})
	t.Run("ListenForSwap", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		require.NoError(t, sc.Restart(ctx))
		l, err := sc.addListener(leSwap)
		require.NoError(t, err)
		select {
		case e := <-l:
			require.True(t, (leSwap&e) > 0, "expected success or gc signal")
		}
	})
	t.Run("ListenForStop", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		require.NoError(t, sc.Restart(ctx))
		l, err := sc.addListener(leUnknown)
		require.NoError(t, err)
		<-sc.Stop()
		select {
		case e := <-l:
			require.Equal(t, e, leStop)
		default:
			t.Fatal("expected listener to recv stop")
		}
	})
	t.Run("ListenerFailsIfStopped", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		require.NoError(t, sc.Restart(ctx))
		<-sc.Stop()
		_, err := sc.addListener(leUnknown)
		require.ErrorIs(t, err, ErrStatsIssuerPaused)
	})
	t.Run("ListenerFailsIfClosed", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		sc.Close()
		require.Error(t, sc.Restart(ctx))
		_, err := sc.addListener(leUnknown)
		require.ErrorIs(t, err, ErrStatsIssuerPaused)
	})
	t.Run("WaitBlocksOnStatsCollection", func(t *testing.T) {
		sqlCtx, sqlEng, sc := emptySetup(t, bthreads, true, true)
		_, orig, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsJobInterval)
		sql.SystemVariables.SetGlobal(sqlCtx, dsess.DoltStatsJobInterval, "60000000000")
		defer func() {
			sql.SystemVariables.SetGlobal(sqlCtx, dsess.DoltStatsJobInterval, orig)
		}()
		require.NoError(t, executeQuery(sqlCtx, sqlEng, "create table xy (x int primary key, y int)"))
		require.NoError(t, sc.Restart(ctx))
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := sc.waitForSignal(ctx, leSwap, 1)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("WaitReturnsIfStoppedBefore", func(t *testing.T) {
		sqlCtx, sqlEng, sc := emptySetup(t, bthreads, true, true)
		require.NoError(t, executeQuery(sqlCtx, sqlEng, "create table xy (x int primary key, y int)"))
		require.NoError(t, sc.Restart(ctx))
		<-sc.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := sc.waitForSignal(ctx, leSwap, 1)
		require.ErrorIs(t, err, ErrStatsIssuerPaused)
	})
	t.Run("WaitHangsUntilCycleCompletes", func(t *testing.T) {
		sqlCtx, sqlEng, sc := emptySetup(t, bthreads, true, true)
		require.NoError(t, executeQuery(sqlCtx, sqlEng, "create table xy (x int primary key, y int)"))
		require.NoError(t, sc.Restart(ctx))
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := sc.waitForSignal(ctx, leSwap, 1)
		require.NoError(t, err)
	})
}
