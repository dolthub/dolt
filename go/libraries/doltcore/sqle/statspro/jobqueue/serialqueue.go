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

package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/circular"
)

// A SerialQueue is a job queue which runs one job at a time. Jobs are
// run in the order they are submitted, with the exception that every
// interrupt job is run before any normal priority job.
//
// A SerialQueue can be paused, in which case it will accept new
// submissions, but will not run them until it is started again.
//
// A SerialQueue can be purged, which deletes any pending jobs from
// it.
//
// A SerialQueue can be stopped, in which case it will not accept new
// submissions and no pending work will be run. Stopping a queue does
// not purge it, but it is easy for a caller to stop and purge the
// queue.
//
// A stopped or paused SerialQueue can be started, which will cause it
// to start running submitted jobs again, including any unpurged jobs
// which were pending when it was stopped or paused.
//
// A SerialQueue runs background threads to coordinate its
// behavior. These background threads are launched with a `Context`
// supplied to its |Run| method. If that `Context` ever becomes
// `Done`, the SerialQueue termainally enters a completed state.
//
// In general, jobs running on the queue should not block indefinitely
// and should be very careful about any synchronization. It is safe
// for jobs within the queue to call DoAsync, InterruptAsync, Stop,
// Pause, Purge and Start on the queue itself. It is a deadlock for a
// job within the queue to perform a DoSync or InterruptSync on the
// queue itself, although that deadlock may be resolved if the
// provided |ctx| ends up |Done|.
type SerialQueue struct {
	running atomic.Bool

	// If the queue is terminally completed, this will be closed.
	// Submissions to the queue scheduler select on this channel
	// to return errors if the scheduler is no longer accepting
	// work.
	completed chan struct{}

	runnerCh chan work
	schedCh  chan schedReq
	errCb    func(error)
}

// |work| represents work to be run on the runner goroutine.
type work struct {
	// The function to call.
	f func() error
	// The channel to close after the work is run.
	done chan struct{}
	// Update worker rate
	newRate time.Duration
}

type schedState int

const (
	// When scheduler is running, it is willing to accept new work
	// and to give work to the work thread.
	schedState_Running schedState = iota
	// When scheduler is paused, it is willing to accept new work
	// but it does not give work to the work thread.
	schedState_Paused
	// When scheduler is stopped, it does not accept new work
	// and it does not give work to the work thread.
	schedState_Stopped
)

type schedReqType int

const (
	schedReqType_Enqueue schedReqType = iota
	schedReqType_Purge
	schedReqType_Start
	schedReqType_Pause
	schedReqType_Stop
)

type schedPriority int

const (
	schedPriority_Normal schedPriority = iota
	schedPriority_High
)

// Incoming message for the scheduler thread.
type schedReq struct {
	reqType schedReqType
	// Always set, the scheduler's response is
	// sent through this channel. The send
	// must never block.
	resp chan schedResp
	// Set when |reqType| is Enqueue
	pri schedPriority
	// Set when |reqType| is Enqueue
	work work
}

type schedResp struct {
	err error
}

var ErrStoppedQueue = errors.New("stopped queue: cannot submit work to a stopped queue.")
var ErrCompletedQueue = errors.New("completed queue: the queue is no longer running.")

// Create a new serial queue. All of the methods on the returned
// SerialQueue block indefinitely until its |Run| method is called.
func NewSerialQueue() *SerialQueue {
	return &SerialQueue{
		completed: make(chan struct{}),
		runnerCh:  make(chan work),
		schedCh:   make(chan schedReq),
	}
}
func (s *SerialQueue) WithErrorCb(errCb func(error)) *SerialQueue {
	s.errCb = errCb
	return s
}

// Run the serial queue's background threads with this |ctx|. If the
// |ctx| ever becomes |Done|, the queue enters a terminal completed
// state. It is an error to call this function more than once.
func (s *SerialQueue) Run(ctx context.Context) {
	if !s.running.CompareAndSwap(false, true) {
		panic("Cannot run a SerialQueue more than once.")
	}
	defer close(s.completed)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.runScheduler(ctx)
	}()
	go func() {
		defer wg.Done()
		s.runRunner(ctx)
	}()
	wg.Wait()
}

// Start the queue. The queue can be in any state, including already started.
func (s *SerialQueue) Start() error {
	return s.makeReq(schedReq{
		reqType: schedReqType_Start,
		resp:    make(chan schedResp, 1),
	})
}

// Pause the queue. The queue can be in any state, including already
// paused.  Note that pausing the queue does not block on any
// currently running job to complete. A pattern to pause the queue
// with a guarantee that nothing is currently running is:
//
// s.InterruptSync(context.Background(), func() { s.Pause() })
func (s *SerialQueue) Pause() error {
	return s.makeReq(schedReq{
		reqType: schedReqType_Pause,
		resp:    make(chan schedResp, 1),
	})
}

// Stop the queue. The queue can be in any state, including already
// stopped.  Note that stopping the queue does not block on any
// currently running job to complete.
func (s *SerialQueue) Stop() error {
	return s.makeReq(schedReq{
		reqType: schedReqType_Stop,
		resp:    make(chan schedResp, 1),
	})
}

// Purge the queue. All pending jobs will be dropped.
func (s *SerialQueue) Purge() error {
	return s.makeReq(schedReq{
		reqType: schedReqType_Purge,
		resp:    make(chan schedResp, 1),
	})
}

func (s *SerialQueue) NewRateLimit(rate time.Duration) error {
	return s.makeReq(schedReq{
		reqType: schedReqType_Enqueue,
		pri:     schedPriority_High,
		work: work{
			f:       func() error { return nil },
			done:    make(chan struct{}),
			newRate: rate,
		},
		resp: make(chan schedResp, 1),
	})
}

// Run a high priority job on the SerialQueue, blocking for its completion.
// If done against a Paused queue, this could block indefinitely. The
// block for completion is gated on the |ctx|.
func (s *SerialQueue) InterruptSync(ctx context.Context, f func() error) error {
	w, err := s.submitWork(schedPriority_High, f)
	if err != nil {
		return err
	}
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.completed:
		return ErrCompletedQueue
	}
}

// Run a normal priority job on the SerialQueue, blocking for its completion.
// When done against a paused queue, this can block indefinitely.
func (s *SerialQueue) DoSync(ctx context.Context, f func() error) error {
	w, err := s.submitWork(schedPriority_Normal, f)
	if err != nil {
		return err
	}
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.completed:
		return ErrCompletedQueue
	}
}

// DoSyncSessionAware initializes a session command before running
// a worker callback. If the context is cancelled midway we either
// finish calling the function, or return without calling the function.
// No return leaves the session in an incomplete state.
func (s *SerialQueue) DoSyncSessionAware(ctx *sql.Context, f func() error) error {
	started := atomic.Bool{}
	nf := func() error {
		if started.Swap(true) {
			return nil
		}
		sql.SessionCommandBegin(ctx.Session)
		defer sql.SessionCommandEnd(ctx.Session)
		return f()
	}
	w, err := s.submitWork(schedPriority_Normal, nf)
	if err != nil {
		return err
	}
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		if started.Swap(true) {
			<-w.done
		}
		return context.Cause(ctx)
	case <-s.completed:
		return ErrCompletedQueue
	}
}

// Run a high priority job asynchronously on the queue. Returns once the
// job is accepted.
func (s *SerialQueue) InterruptAsync(f func() error) error {
	_, err := s.submitWork(schedPriority_High, f)
	if err != nil {
		return err
	}
	return nil
}

// Run a normal priority job asynchronously on the queue. Returns once the
// job is accepted.
func (s *SerialQueue) DoAsync(f func() error) error {
	_, err := s.submitWork(schedPriority_Normal, f)
	if err != nil {
		return err
	}
	return nil
}

// Helper function to submit work. Returns the work submitted, if it
// was successful, and an error otherwise.
func (s *SerialQueue) submitWork(pri schedPriority, f func() error) (work, error) {
	w := work{
		f:    f,
		done: make(chan struct{}),
	}
	err := s.makeReq(schedReq{
		reqType: schedReqType_Enqueue,
		pri:     pri,
		work:    w,
		resp:    make(chan schedResp, 1),
	})
	if err != nil {
		return work{}, err
	}
	return w, nil
}

func (s *SerialQueue) makeReq(req schedReq) error {
	select {
	case s.schedCh <- req:
		resp := <-req.resp
		return resp.err
	case <-s.completed:
		return ErrCompletedQueue
	}
}

// Read off the input channels and maintain queues of pending work.
// Deliver that work to the runner channel if it is desired.
func (s *SerialQueue) runScheduler(ctx context.Context) {
	state := schedState_Running
	normalQ := circular.NewBuff[work](16)
	highQ := circular.NewBuff[work](16)
	for {
		var sendWorkCh chan work
		var sendWork work
		var sentWorkCallback func()

		if state == schedState_Running {
			if highQ.Len() > 0 {
				sendWorkCh = s.runnerCh
				sendWork = highQ.Front()
				sentWorkCallback = highQ.Pop
			} else if normalQ.Len() > 0 {
				sendWorkCh = s.runnerCh
				sendWork = normalQ.Front()
				sentWorkCallback = normalQ.Pop
			}
		}

		select {
		case msg := <-s.schedCh:
			switch msg.reqType {
			case schedReqType_Enqueue:
				if state == schedState_Stopped {
					msg.resp <- schedResp{
						err: ErrStoppedQueue,
					}
				} else {
					if msg.pri == schedPriority_High {
						highQ.Push(msg.work)
					} else {
						normalQ.Push(msg.work)
					}
					msg.resp <- schedResp{
						err: nil,
					}
				}
			case schedReqType_Purge:
				highQ = circular.NewBuff[work](highQ.Cap())
				normalQ = circular.NewBuff[work](normalQ.Cap())
				msg.resp <- schedResp{
					err: nil,
				}
			case schedReqType_Start:
				state = schedState_Running
				msg.resp <- schedResp{
					err: nil,
				}
			case schedReqType_Pause:
				state = schedState_Paused
				msg.resp <- schedResp{
					err: nil,
				}
			case schedReqType_Stop:
				state = schedState_Stopped
				msg.resp <- schedResp{
					err: nil,
				}
			}
		case sendWorkCh <- sendWork:
			// Pop from queue the work came from.
			sentWorkCallback()
		case <-ctx.Done():
			return
		}
	}
}

// Read off the runner channel and run the submitted work.
func (s *SerialQueue) runRunner(ctx context.Context) {
	ticker := time.NewTicker(1)
	for {
		select {
		case w := <-s.runnerCh:
			if w.newRate > 0 {
				ticker.Reset(w.newRate)
			}

			// do not run jobs more frequently than the ticker rate
			select {
			case <-ticker.C:
			case <-ctx.Done():
			}

			func() {
				var err error
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("serialQueue panicked running work: %s", r)
					}
					if err != nil {
						s.errCb(err)
					}
				}()
				err = w.f()
			}()
			close(w.done)
		case <-ctx.Done():
			return
		}
	}
}
