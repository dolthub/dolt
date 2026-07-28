// Copyright 2023 Dolthub, Inc.
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

package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/store/datas"
)

func TestCommitHookStartsNotCaughtUp(t *testing.T) {
	srcEnv := dtestutils.CreateTestEnv()
	ctx := context.Background()
	t.Cleanup(func() {
		srcEnv.Close()
	})
	destEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() {
		destEnv.Close()
	})

	hook := newCommitHook(logrus.StandardLogger(), "origin", "https://localhost:50051/mydb", "mydb", RolePrimary, func(context.Context) (*doltdb.DoltDB, error) {
		return destEnv.DoltDB(ctx), nil
	}, srcEnv.DoltDB(ctx), t.TempDir())

	require.False(t, hook.isCaughtUp())
}

// fakeClock is a manually-advanced clock used to make the commithook's probe
// scheduling deterministic in tests. It is safe for concurrent use because the
// wait closure reads it from a separate goroutine.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// failWait deterministically simulates a replication wait that times out after
// the ack timeout: it runs waitF, advances the clock by budget, then cancels
// the context with doltdb.ErrReplicationWaitFailed exactly as the ack-writes
// machinery does. The clock is advanced before the cancel, and the closure only
// reads its end time after the context is canceled, so the observed budget is
// deterministic.
func failWait(c *fakeClock, waitF func(context.Context) error, budget time.Duration) error {
	ctx, cancel := context.WithCancelCause(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- waitF(ctx)
	}()
	c.advance(budget)
	cancel(doltdb.ErrReplicationWaitFailed)
	return <-errc
}

// progressWait deterministically simulates a replication push completing within
// the ack timeout: it runs waitF and delivers replication progress via the
// progress notifier. The waiter's channel is registered synchronously in
// Execute before waitF is run here, so RecordSuccess reliably wakes it.
func progressWait(hook *commithook, waitF func(context.Context) error) error {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	errc := make(chan error, 1)
	go func() {
		errc <- waitF(ctx)
	}()
	hook.mu.Lock()
	a := hook.progressNotifier.BeginAttempt()
	hook.progressNotifier.RecordSuccess(a)
	hook.mu.Unlock()
	return <-errc
}

// TestCommitHookCircuitBreakerClosesWhileBusy proves the circuit breaker can
// close once replication is progressing within the ack timeout, even though the
// replication thread never fully quiesces because writes keep arriving.
//
// The hook is a primary that has never fully trued up its standby, so it is
// never "caught up" for the duration of the test. We drive Execute() directly
// and simulate replication *progress* (a completed push) without ever marking
// ourselves caught up, modeling a continuously-written-to primary.
//
// Before the probe change, once the breaker opened it could only close when the
// replication thread fully quiesced (nextHead == lastPushedHead), which never
// happens here -- so every subsequent commit fast-failed forever and the final
// probe assertion (require.NoError) fails. That is the red/green boundary.
func TestCommitHookCircuitBreakerClosesWhileBusy(t *testing.T) {
	srcEnv := dtestutils.CreateTestEnv()
	ctx := context.Background()
	t.Cleanup(func() { srcEnv.Close() })
	destEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() { destEnv.Close() })

	clock := &fakeClock{t: time.Unix(1000, 0)}

	hook := newCommitHook(logrus.StandardLogger(), "origin", "https://localhost:50051/mydb", "mydb", RolePrimary, func(context.Context) (*doltdb.DoltDB, error) {
		return destEnv.DoltDB(ctx), nil
	}, srcEnv.DoltDB(ctx), t.TempDir())
	hook.nowFunc = clock.now

	// A primary that has never pushed is not caught up, and stays that way
	// for the whole test: we never set lastPushedHead, so we model writes
	// that arrive faster than the standby fully trues up.
	require.False(t, hook.isCaughtUp())

	const ackBudget = 5 * time.Second

	// 1. The first commit's replication wait times out. In production this is
	//    what opens the breaker: the wait returns ErrReplicationWaitFailed and
	//    the ack-writes machinery then calls NotifyWaitFailed().
	waitF, err := hook.Execute(ctx, datas.Dataset{}, srcEnv.DoltDB(ctx))
	require.NoError(t, err)
	require.NotNil(t, waitF)
	require.ErrorIs(t, failWait(clock, waitF, ackBudget), doltdb.ErrReplicationWaitFailed)
	hook.NotifyWaitFailed()

	hook.mu.Lock()
	require.True(t, hook.fastFailReplicationWait)
	probeAt := hook.nextProbeAt
	hook.mu.Unlock()
	// The probe was scheduled in the future, spaced by the observed budget.
	require.True(t, probeAt.After(clock.now()))

	// 2. While the breaker is open and before the next probe is due, commits
	//    fast-fail immediately without blocking, even given a live context.
	waitF, err = hook.Execute(ctx, datas.Dataset{}, srcEnv.DoltDB(ctx))
	require.NoError(t, err)
	require.NotNil(t, waitF)
	require.Error(t, waitF(context.Background()))

	// 3. Advance the clock past the scheduled probe time.
	clock.set(probeAt.Add(time.Nanosecond))

	// 4. The next commit is issued as a real probe. Replication makes progress
	//    (a push completes) but we never mark ourselves caught up. The probe
	//    observes that progress within the ack timeout and closes the breaker.
	waitF, err = hook.Execute(ctx, datas.Dataset{}, srcEnv.DoltDB(ctx))
	require.NoError(t, err)
	require.NotNil(t, waitF)
	require.NoError(t, progressWait(hook, waitF))

	// The improvement: the breaker is closed even though we never fully
	// quiesced.
	require.False(t, hook.isCaughtUp())
	hook.mu.Lock()
	require.False(t, hook.fastFailReplicationWait)
	hook.mu.Unlock()

	// 5. With the breaker closed, subsequent commits block on real progress
	//    again rather than fast-failing.
	waitF, err = hook.Execute(ctx, datas.Dataset{}, srcEnv.DoltDB(ctx))
	require.NoError(t, err)
	require.NotNil(t, waitF)
	require.ErrorIs(t, failWait(clock, waitF, ackBudget), doltdb.ErrReplicationWaitFailed)
}
