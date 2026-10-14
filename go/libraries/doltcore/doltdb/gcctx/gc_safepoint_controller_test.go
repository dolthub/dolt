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

package gcctx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

func TestGCSafepointController(t *testing.T) {
	t.Parallel()
	t.Run("SessionEnd", func(t *testing.T) {
		t.Parallel()
		t.Run("UnknownSession", func(t *testing.T) {
			t.Parallel()
			controller := NewGCSafepointController()
			controller.SessionEnd(&visitable{})
		})
		t.Run("KnownSession", func(t *testing.T) {
			t.Parallel()
			controller := NewGCSafepointController()
			sess := &visitable{}
			controller.SessionCommandBegin(sess)
			controller.SessionCommandEnd(sess)
			controller.SessionEnd(sess)
		})
		t.Run("RunningSession", func(t *testing.T) {
			t.Parallel()
			controller := NewGCSafepointController()
			sess := &visitable{}
			controller.SessionCommandBegin(sess)
			require.Panics(t, func() {
				controller.SessionEnd(sess)
			})
		})
	})
	t.Run("CommandBegin", func(t *testing.T) {
		t.Parallel()
		t.Run("RunningSession", func(t *testing.T) {
			t.Parallel()
			controller := NewGCSafepointController()
			sess := &visitable{}
			controller.SessionCommandBegin(sess)
			require.Panics(t, func() {
				controller.SessionCommandBegin(sess)
			})
		})
		t.Run("AfterCommandEnd", func(t *testing.T) {
			t.Parallel()
			controller := NewGCSafepointController()
			sess := &visitable{}
			controller.SessionCommandBegin(sess)
			controller.SessionCommandEnd(sess)
			controller.SessionCommandBegin(sess)
		})
	})
	t.Run("CommandEnd", func(t *testing.T) {
		t.Parallel()
		t.Run("NotKnown", func(t *testing.T) {
			t.Parallel()
			controller := NewGCSafepointController()
			sess := &visitable{}
			require.Panics(t, func() {
				controller.SessionCommandEnd(sess)
			})
		})
		t.Run("NotRunning", func(t *testing.T) {
			t.Parallel()
			controller := NewGCSafepointController()
			sess := &visitable{}
			controller.SessionCommandBegin(sess)
			controller.SessionCommandEnd(sess)
			require.Panics(t, func() {
				controller.SessionCommandEnd(sess)
			})
		})
	})
	t.Run("Waiter", func(t *testing.T) {
		t.Parallel()
		t.Run("Empty", func(t *testing.T) {
			t.Parallel()
			var nilCh chan struct{}
			block := func(context.Context, GCRootsProvider) error {
				<-nilCh
				return nil
			}
			controller := NewGCSafepointController()
			waiter := controller.Waiter(context.Background(), nil, block)
			waiter.Wait(context.Background())
		})
		t.Run("OnlyThisSession", func(t *testing.T) {
			t.Parallel()
			var nilCh chan struct{}
			block := func(context.Context, GCRootsProvider) error {
				<-nilCh
				return nil
			}
			sess := &visitable{}
			controller := NewGCSafepointController()
			controller.SessionCommandBegin(sess)
			waiter := controller.Waiter(context.Background(), sess, block)
			waiter.Wait(context.Background())
			controller.SessionCommandEnd(sess)
			controller.SessionEnd(sess)
		})
		t.Run("OneQuiescedOneNot", func(t *testing.T) {
			t.Parallel()
			// A test case where one session is known
			// but not within a command and another one
			// is within a command at the time the
			// waiter is created.
			quiesced := &visitable{}
			running := &visitable{}
			controller := NewGCSafepointController()
			controller.SessionCommandBegin(quiesced)
			controller.SessionCommandBegin(running)
			controller.SessionCommandEnd(quiesced)
			sawQuiesced, sawRunning, waitDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			wait := func(_ context.Context, s GCRootsProvider) error {
				if s == quiesced {
					close(sawQuiesced)
				} else if s == running {
					close(sawRunning)
				} else {
					panic("saw unexpected session")
				}
				return nil
			}
			waiter := controller.Waiter(context.Background(), nil, wait)
			go func() {
				waiter.Wait(context.Background())
				close(waitDone)
			}()
			<-sawQuiesced
			select {
			case <-sawRunning:
				require.FailNow(t, "unexpected saw running session on callback before it was quiesced")
			case <-time.After(50 * time.Millisecond):
			}
			controller.SessionCommandEnd(running)
			<-sawRunning
			<-waitDone

			controller.SessionCommandBegin(quiesced)
			controller.SessionCommandBegin(running)
			controller.SessionCommandEnd(quiesced)
			controller.SessionCommandEnd(running)
		})
		t.Run("OneQuiescedOneNotCanceledContext", func(t *testing.T) {
			t.Parallel()
			// When the Wait context is canceled, we do not block on
			// the running sessions and they never get visited.
			quiesced := &visitable{}
			running := &visitable{}
			controller := NewGCSafepointController()
			controller.SessionCommandBegin(quiesced)
			controller.SessionCommandBegin(running)
			controller.SessionCommandEnd(quiesced)
			sawQuiesced, sawRunning, waitDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			wait := func(_ context.Context, s GCRootsProvider) error {
				if s == quiesced {
					close(sawQuiesced)
				} else if s == running {
					close(sawRunning)
				} else {
					panic("saw unexpected session")
				}
				return nil
			}
			waiter := controller.Waiter(context.Background(), nil, wait)
			var waitErr error
			go func() {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				waitErr = waiter.Wait(ctx)
				close(waitDone)
			}()
			<-sawQuiesced
			<-waitDone
			require.Error(t, waitErr)
			select {
			case <-sawRunning:
				require.FailNow(t, "unexpected saw running session on callback before it was quiesced")
			case <-time.After(50 * time.Millisecond):
			}
			controller.SessionCommandEnd(running)
			select {
			case <-sawRunning:
				require.FailNow(t, "unexpected saw running session on callback before it was quiesced")
			case <-time.After(50 * time.Millisecond):
			}

			controller.SessionCommandBegin(quiesced)
			controller.SessionCommandBegin(running)
			controller.SessionCommandEnd(quiesced)
			controller.SessionCommandEnd(running)
		})
		t.Run("BeginBlocksUntilVisitFinished", func(t *testing.T) {
			t.Parallel()
			quiesced := &visitable{}
			running := &visitable{}
			controller := NewGCSafepointController()
			controller.SessionCommandBegin(quiesced)
			controller.SessionCommandEnd(quiesced)
			controller.SessionCommandBegin(running)
			finishQuiesced, finishRunning := make(chan struct{}), make(chan struct{})
			sawQuiesced, sawRunning := make(chan struct{}), make(chan struct{})
			wait := func(_ context.Context, s GCRootsProvider) error {
				if s == quiesced {
					close(sawQuiesced)
					<-finishQuiesced
				} else if s == running {
					close(sawRunning)
					<-finishRunning
				} else {
					panic("saw unexpected session")
				}
				return nil
			}
			waiter := controller.Waiter(context.Background(), nil, wait)
			waitDone := make(chan struct{})
			go func() {
				waiter.Wait(context.Background())
				close(waitDone)
			}()
			beginDone := make(chan struct{})
			go func() {
				controller.SessionCommandBegin(quiesced)
				close(beginDone)
			}()
			<-sawQuiesced
			select {
			case <-beginDone:
				require.FailNow(t, "unexpected beginDone")
			case <-time.After(50 * time.Millisecond):
			}

			newSession := &visitable{}
			controller.SessionCommandBegin(newSession)
			controller.SessionCommandEnd(newSession)
			controller.SessionEnd(newSession)

			close(finishQuiesced)
			<-beginDone
			beginDone = make(chan struct{})
			go func() {
				controller.SessionCommandEnd(running)
				<-sawRunning
				controller.SessionCommandBegin(running)
				close(beginDone)
			}()
			select {
			case <-beginDone:
				require.FailNow(t, "unexpected beginDone")
			case <-time.After(50 * time.Millisecond):
			}
			close(finishRunning)
			<-beginDone

			<-waitDone

			controller.SessionCommandEnd(quiesced)
			controller.SessionCommandEnd(running)
			controller.SessionCommandBegin(quiesced)
			controller.SessionCommandBegin(running)
			controller.SessionCommandEnd(quiesced)
			controller.SessionCommandEnd(running)

			controller.SessionEnd(quiesced)
			controller.SessionEnd(running)
			err := controller.Waiter(context.Background(), nil, func(context.Context, GCRootsProvider) error {
				panic("unexpected registered session")
			}).Wait(context.Background())
			require.NoError(t, err)
		})
	})
}

func TestGCSafepointConcurrentWaiters(t *testing.T) {
	t.Parallel()
	for _, running := range []bool{false, true} {
		name := "quiesced"
		if running {
			name = "running"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			controller := NewGCSafepointController()
			sess := &visitable{}
			require.NoError(t, controller.SessionCommandBegin(sess))
			if !running {
				controller.SessionCommandEnd(sess)
			}

			firstStarted, secondStarted := make(chan struct{}), make(chan struct{})
			finishFirst, finishSecond := make(chan struct{}), make(chan struct{})
			firstErr := errors.New("first visit failed")
			first := controller.Waiter(context.Background(), nil, func(_ context.Context, got GCRootsProvider) error {
				if got != sess {
					panic("visited unexpected session")
				}
				close(firstStarted)
				<-finishFirst
				return firstErr
			})
			second := controller.Waiter(context.Background(), nil, func(_ context.Context, got GCRootsProvider) error {
				if got != sess {
					panic("visited unexpected session")
				}
				close(secondStarted)
				<-finishSecond
				return nil
			})
			if running {
				select {
				case <-firstStarted:
					t.Fatal("visited during command")
				case <-secondStarted:
					t.Fatal("visited during command")
				default:
				}
				controller.SessionCommandEnd(sess)
			}
			<-firstStarted
			<-secondStarted

			firstWaitDone, secondWaitDone := make(chan error, 1), make(chan error, 1)
			go func() { firstWaitDone <- first.Wait(context.Background()) }()
			go func() { secondWaitDone <- second.Wait(context.Background()) }()
			beginDone := make(chan struct{})
			go func() {
				controller.SessionCommandBegin(sess)
				close(beginDone)
			}()
			close(finishFirst)
			require.ErrorIs(t, <-firstWaitDone, firstErr)
			select {
			case <-beginDone:
				t.Fatal("command began while second visit was active")
			case <-secondWaitDone:
				t.Fatal("second wait returned before its visit finished")
			default:
			}
			close(finishSecond)
			require.NoError(t, <-secondWaitDone)
			<-beginDone
			controller.SessionCommandEnd(sess)
			controller.SessionEnd(sess)
		})
	}
}

func TestGCSafepointCancelOnlyOwnPendingVisits(t *testing.T) {
	t.Parallel()
	controller := NewGCSafepointController()
	sess := &visitable{}
	require.NoError(t, controller.SessionCommandBegin(sess))
	firstVisited := make(chan struct{})
	first := controller.Waiter(context.Background(), nil, func(context.Context, GCRootsProvider) error {
		close(firstVisited)
		return nil
	})
	secondVisited := make(chan struct{})
	second := controller.Waiter(context.Background(), nil, func(context.Context, GCRootsProvider) error {
		close(secondVisited)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, first.Wait(ctx), context.Canceled)
	controller.SessionCommandEnd(sess)
	require.NoError(t, second.Wait(context.Background()))
	<-secondVisited
	select {
	case <-firstVisited:
		t.Fatal("canceled waiter visited the session")
	default:
	}
	controller.SessionEnd(sess)
}

func TestGCSafepointCanceledWaitBlocksForStartedVisit(t *testing.T) {
	t.Parallel()
	controller := NewGCSafepointController()
	sess := &visitable{}
	require.NoError(t, controller.SessionCommandBegin(sess))
	controller.SessionCommandEnd(sess)
	started, finish, waitDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	waiter := controller.Waiter(context.Background(), nil, func(context.Context, GCRootsProvider) error {
		close(started)
		<-finish
		return nil
	})
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	go func() { waitDone <- waiter.Wait(ctx) }()
	select {
	case <-waitDone:
		t.Fatal("canceled wait returned before the active visit finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(finish)
	require.ErrorIs(t, <-waitDone, context.Canceled)
	controller.SessionEnd(sess)
}

func TestGCSafepointConcurrentGCCommands(t *testing.T) {
	t.Parallel()
	controller := NewGCSafepointController()
	firstSession, secondSession := &visitable{}, &visitable{}
	require.NoError(t, controller.SessionCommandBegin(firstSession))
	require.NoError(t, controller.SessionCommandBegin(secondSession))

	firstVisitStarted, secondVisitStarted := make(chan struct{}), make(chan struct{})
	finishFirstVisit, finishSecondVisit := make(chan struct{}), make(chan struct{})
	first := controller.Waiter(context.Background(), firstSession, func(_ context.Context, s GCRootsProvider) error {
		if s != secondSession {
			panic("first GC visited unexpected session")
		}
		close(firstVisitStarted)
		<-finishFirstVisit
		return nil
	})
	second := controller.Waiter(context.Background(), secondSession, func(_ context.Context, s GCRootsProvider) error {
		if s != firstSession {
			panic("second GC visited unexpected session")
		}
		close(secondVisitStarted)
		<-finishSecondVisit
		return nil
	})
	// Both visits must run while the two GC commands are still open.
	select {
	case <-firstVisitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first GC could not visit the other GC session")
	}
	select {
	case <-secondVisitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("second GC could not visit the other GC session")
	}

	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- first.Wait(context.Background()) }()
	go func() { secondDone <- second.Wait(context.Background()) }()
	close(finishFirstVisit)
	select {
	case <-firstDone:
		t.Fatal("first GC resumed while its session was still being visited")
	default:
	}
	close(finishSecondVisit)
	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("first GC did not finish waiting")
	}
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("second GC did not finish waiting")
	}
	controller.SessionCommandEnd(firstSession)
	controller.SessionCommandEnd(secondSession)
}

type visitable struct {
	// Give it an unused memory so it gets a unique address.
	state bool
}

func (*visitable) VisitGCRoots(ctx context.Context, db string, keep func(hash.Hash) bool) error {
	return nil
}
