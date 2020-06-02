// Copyright 2020 Liquidata, Inc.
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

package async

import (
	"container/list"
	"context"
	"fmt"
	"sync"
)

// Action is the function called by an ActionExecutor on each given value.
type Action func(ctx context.Context, val interface{}) error

// ActionExecutor is designed for asynchronous workloads that should run when a new task is available. The closest analog
// would be to have a long-running goroutine that receives from a channel, however ActionExecutor provides three major
// points of differentiation. The first is that there is no need to close the queue, as goroutines automatically exit
// when the queue is empty. The second is that a concurrency parameter may be set, that will spin up goroutines as
// needed until the maximum number is attained. The third is that you don't have to declare the buffer size beforehand
// as with channels, allowing the queue to respond to demand. You may declare a max buffer though, for RAM-limited
// situations, which then blocks appends until the buffer is below the max given.
type ActionExecutor struct {
	action      Action
	ctx         context.Context
	concurrency uint32
	err         error
	finished    *WaitGroup
	linkedList  *list.List
	running     uint32
	maxBuffer   uint64
	syncCond    *sync.Cond
}

// NewActionExecutor returns an ActionExecutor that will run the given action on each appended value, and run up to the max
// number of goroutines as defined by concurrency. If concurrency is 0, then it is set to 1. If maxBuffer is 0, then it
// is unlimited. Panics on a nil action.
func NewActionExecutor(ctx context.Context, action Action, concurrency uint32, maxBuffer uint64) *ActionExecutor {
	if action == nil {
		panic("action cannot be nil")
	}
	if concurrency == 0 {
		concurrency = 1
	}
	return &ActionExecutor{
		action:      action,
		concurrency: concurrency,
		ctx:         ctx,
		finished:    &WaitGroup{},
		linkedList:  list.New(),
		running:     0,
		maxBuffer:   maxBuffer,
		syncCond:    sync.NewCond(&sync.Mutex{}),
	}
}

// Execute adds the value to the end of the queue to be executed. If any action encountered an error before this call,
// then the value is not added and this returns immediately.
func (aq *ActionExecutor) Execute(val interface{}) {
	aq.syncCond.L.Lock()
	defer aq.syncCond.L.Unlock()

	if aq.err != nil { // if we've errored before, then no point in running anything again
		return
	}

	for aq.maxBuffer != 0 && uint64(aq.linkedList.Len()) >= aq.maxBuffer {
		aq.syncCond.Wait()
	}
	aq.finished.Add(1)
	aq.linkedList.PushBack(val)

	if aq.running < aq.concurrency {
		aq.running++
		go aq.work()
	}
}

// WaitForEmpty waits until the queue is empty, and then returns any errors that any actions may have encountered.
func (aq *ActionExecutor) WaitForEmpty() error {
	aq.finished.Wait()
	return aq.err
}

// work runs until the list is empty. If any error occurs from any action, then we do not call any further actions,
// although we still iterate over the list and clear it.
func (aq *ActionExecutor) work() {
	for {
		aq.syncCond.L.Lock() // check element list and valid state, so we lock

		element := aq.linkedList.Front()
		if element == nil {
			aq.running--
			aq.syncCond.L.Unlock() // early exit, so we unlock
			return                 // we don't signal here since the buffer is empty, hence the return in the first place
		}
		_ = aq.linkedList.Remove(element)
		encounteredError := aq.err != nil

		aq.syncCond.Signal()   // if an append is waiting because of a full buffer, we signal for it to continue
		aq.syncCond.L.Unlock() // done checking list and state, so we unlock

		if !encounteredError {
			var err error
			func() { // this func is to capture a potential panic from the action, and present it as an error instead
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("panic in ActionExecutor:\n%v", r)
					}
				}()
				err = aq.action(aq.ctx, element.Value)
			}()
			// Technically, two actions could error at the same time and only one would persist their error. For async
			// tasks, we don't care as much about which action errored, just that an action error.
			if err != nil {
				aq.syncCond.L.Lock()
				aq.err = err
				aq.syncCond.L.Unlock()
			}
		}

		aq.finished.Done()
	}
}
