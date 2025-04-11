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
			controller.SessionCommandBegin(sess, nil)
			controller.SessionCommandEnd(sess)
			controller.SessionEnd(sess)
		})
		t.Run("RunningSession", func(t *testing.T) {
			t.Parallel()
			controller := NewGCSafepointController()
			sess := &visitable{}
			controller.SessionCommandBegin(sess, nil)
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
			controller.SessionCommandBegin(sess, nil)
			require.Panics(t, func() {
				controller.SessionCommandBegin(sess, nil)
			})
		})
		t.Run("AfterCommandEnd", func(t *testing.T) {
			t.Parallel()
			controller := NewGCSafepointController()
			sess := &visitable{}
			controller.SessionCommandBegin(sess, nil)
			controller.SessionCommandEnd(sess)
			controller.SessionCommandBegin(sess, nil)
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
			controller.SessionCommandBegin(sess, nil)
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
			controller.SessionCommandBegin(sess, nil)
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
			controller.SessionCommandBegin(quiesced, nil)
			controller.SessionCommandBegin(running, nil)
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

			controller.SessionCommandBegin(quiesced, nil)
			controller.SessionCommandBegin(running, nil)
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
			controller.SessionCommandBegin(quiesced, nil)
			controller.SessionCommandBegin(running, nil)
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

			controller.SessionCommandBegin(quiesced, nil)
			controller.SessionCommandBegin(running, nil)
			controller.SessionCommandEnd(quiesced)
			controller.SessionCommandEnd(running)
		})
		t.Run("BeginBlocksUntilVisitFinished", func(t *testing.T) {
			t.Parallel()
			quiesced := &visitable{}
			running := &visitable{}
			controller := NewGCSafepointController()
			controller.SessionCommandBegin(quiesced, nil)
			controller.SessionCommandEnd(quiesced)
			controller.SessionCommandBegin(running, nil)
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
				controller.SessionCommandBegin(quiesced, nil)
				close(beginDone)
			}()
			<-sawQuiesced
			select {
			case <-beginDone:
				require.FailNow(t, "unexpected beginDone")
			case <-time.After(50 * time.Millisecond):
			}

			newSession := &visitable{}
			controller.SessionCommandBegin(newSession, nil)
			controller.SessionCommandEnd(newSession)
			controller.SessionEnd(newSession)

			close(finishQuiesced)
			<-beginDone
			beginDone = make(chan struct{})
			go func() {
				controller.SessionCommandEnd(running)
				<-sawRunning
				controller.SessionCommandBegin(running, nil)
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
			controller.SessionCommandBegin(quiesced, nil)
			controller.SessionCommandBegin(running, nil)
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

type visitable struct {
	// Give it an unused memory so it gets a unique address.
	state bool
}

func (*visitable) VisitGCRoots(ctx context.Context, db string, keep func(hash.Hash) bool) error {
	return nil
}
