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

import "sync"

// WaitGroup functions similarly to sync.WaitGroup that ships with Go, with the key difference being that this one
// allows calls to Add with a positive delta to occur while another thread is waiting, while the sync version
// may panic. The tradeoff is a performance reduction since we now lock on all modifications to the counter.
type WaitGroup struct {
	counter  int64 // we allow negative counters and don't panic on them, as it could be useful for the caller
	initOnce sync.Once
	syncCond *sync.Cond
}

// Add adds delta, which may be negative, to the WaitGroup counter. If the counter becomes zero, all goroutines blocked
// on Wait are released. If the counter goes negative, Add panics.
func (wg *WaitGroup) Add(delta int64) {
	wg.init()
	wg.syncCond.L.Lock()
	defer wg.syncCond.L.Unlock()

	wg.counter += delta
	if wg.counter < 0 {
		panic("negative WaitGroup counter")
	} else if wg.counter == 0 {
		wg.syncCond.Broadcast()
	}
}

// Done decrements the WaitGroup counter by one.
func (wg *WaitGroup) Done() {
	wg.Add(-1)
}

// Wait blocks until the WaitGroup counter is less than or equal to zero.
func (wg *WaitGroup) Wait() {
	wg.init()
	wg.syncCond.L.Lock()
	defer wg.syncCond.L.Unlock()

	for wg.counter > 0 {
		wg.syncCond.Wait()
	}
}

// sync.WaitGroup allows the user to use the zero value of a wait group with &sync.WaitGroup{}. Since this is supposed
// to be a drop-in replacement, the user would expect to call &async.WaitGroup{}. Since we need some setup, we make use
// of sync.Once to run that setup the first time the wait group is used.
func (wg *WaitGroup) init() {
	wg.initOnce.Do(func() {
		wg.syncCond = sync.NewCond(&sync.Mutex{})
	})
}
