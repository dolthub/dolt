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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestListening(t *testing.T) {
	bthreads := sql.NewBackgroundThreads()
	defer bthreads.Shutdown()
	t.Run("ClosedDoesNotStart", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		sc.Close()
		require.Error(t, sc.Restart())
		require.Nil(t, sc.activeCtxCancel)
	})
	t.Run("IsStoppable", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		eg := errgroup.Group{}
		ctx := sc.newThreadCtx(context.Background())
		eg.Go(func() error {
			return sc.runIssuer(ctx)
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
		sc.Stop()
		<-ctx.Done()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	})
	t.Run("StopsAreIdempotent", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		eg := errgroup.Group{}
		ctx := sc.newThreadCtx(context.Background())
		eg.Go(func() error {
			return sc.runIssuer(ctx)
		})

		sc.Stop()
		sc.Stop()
		sc.Stop()
		sc.Stop()
		<-ctx.Done()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	})
	t.Run("IsRestartable", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		eg := errgroup.Group{}
		ctx1 := sc.newThreadCtx(context.Background())
		eg.Go(func() error {
			return sc.runIssuer(ctx1)
		})

		ctx2 := sc.newThreadCtx(context.Background())
		eg.Go(func() error {
			return sc.runIssuer(ctx2)
		})

		ctx3 := sc.newThreadCtx(context.Background())
		eg.Go(func() error {
			return sc.runIssuer(ctx3)
		})

		<-ctx1.Done()
		<-ctx2.Done()
		sc.Stop()
		<-ctx3.Done()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	})
	t.Run("ConcurrentStartStopsAreOk", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			for _ = range 20 {
				require.NoError(t, sc.Restart())
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
			for _ = range 20 {
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
		require.NoError(t, sc.Restart())
		l, err := sc.addListener(leSwap)
		require.NoError(t, err)
		select {
		case e := <-l:
			require.True(t, (leSwap&e) > 0, "expected success or gc signal")
		}
	})
	t.Run("ListenForStop", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		require.NoError(t, sc.Restart())
		var l chan listenerEvent
		err := sc.sq.DoSync(context.Background(), func() error {
			// do this in serial queue to make sure we don't race
			// with swap
			var err error
			require.NoError(t, err)
			l, err = sc.addListener(leUnknown)
			require.NoError(t, err)
			sc.Stop()
			return nil
		})
		require.NoError(t, err)
		select {
		case e := <-l:
			require.Equal(t, e, leStop)
		default:
			t.Fatal("expected listener to recv stop")
		}
	})
	t.Run("ListenerFailsIfStopped", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		require.NoError(t, sc.Restart())
		sc.Stop()
		_, err := sc.addListener(leUnknown)
		require.ErrorIs(t, err, ErrStatsIssuerPaused)
	})
	t.Run("ListenerFailsIfClosed", func(t *testing.T) {
		sc := newStatsCoord(bthreads)
		sc.Close()
		require.Error(t, sc.Restart())
		_, err := sc.addListener(leUnknown)
		require.ErrorIs(t, err, ErrStatsIssuerPaused)
	})
	t.Run("WaitBlocksOnStatsCollection", func(t *testing.T) {
		sqlCtx, sqlEng, sc := emptySetup(t, bthreads, true)
		require.NoError(t, executeQuery(sqlCtx, sqlEng, "create table xy (x int primary key, y int)"))
		require.NoError(t, sc.Restart())
		done := make(chan struct{})
		wg := sync.WaitGroup{}
		wg.Add(2)
		sc.sq.DoAsync(func() error {
			defer wg.Done()
			<-done
			return nil
		})
		go func() {
			defer wg.Done()
			defer close(done)
			ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond)
			err := sc.waitForCond(ctx, leSwap, 1)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}()
		wg.Wait()
	})
	t.Run("WaitReturnsIfStoppedBefore", func(t *testing.T) {
		sqlCtx, sqlEng, sc := emptySetup(t, bthreads, true)
		require.NoError(t, executeQuery(sqlCtx, sqlEng, "create table xy (x int primary key, y int)"))
		require.NoError(t, sc.Restart())
		done := make(chan struct{})
		wg := sync.WaitGroup{}
		wg.Add(2)
		sc.sq.DoAsync(func() error {
			defer wg.Done()
			<-done
			return nil
		})
		go func() {
			defer wg.Done()
			defer close(done)
			sc.Stop()
			ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond)
			err := sc.waitForCond(ctx, leSwap, 1)
			require.ErrorIs(t, err, ErrStatsIssuerPaused)
		}()
		wg.Wait()
	})
	t.Run("WaitHangsUntilCycleCompletes", func(t *testing.T) {
		sqlCtx, sqlEng, sc := emptySetup(t, bthreads, true)
		require.NoError(t, executeQuery(sqlCtx, sqlEng, "create table xy (x int primary key, y int)"))
		require.NoError(t, sc.Restart())
		done := make(chan struct{})
		wg := sync.WaitGroup{}
		wg.Add(2)
		sc.sq.DoAsync(func() error {
			defer wg.Done()
			<-done
			return nil
		})
		go func() {
			defer wg.Done()
			ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond)
			err := sc.waitForCond(ctx, leSwap, 1)
			require.NoError(t, err)
		}()
		close(done)
		wg.Wait()
	})
}
