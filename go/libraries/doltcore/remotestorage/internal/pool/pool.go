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

package pool

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sync/errgroup"
)

type Func func(ctx context.Context, shutdown <-chan struct{}) error

// A |pool.Dynamic| is a thread pool which can be dynamically sized from the
// outside. It is created with a |Func|, which represents the thread function
// to run on each worker thread. That |Func| must adhere to a contract where if
// it reads from the supplied |shutdown| channel, it shutsdown in a timely
// manner.
//
// To run the threads in the pool, call |Run|.
//
// To set the desired size of the pool, call |SetSize|. If |size| is larger
// than the current size, new threads will be created immediately to bring the
// size up to the requested size. If |size| is smaller than the requested size,
// then |curSize-size| sends on |shutdown| will be performed, to bring
// |curSize| down to |size|. |SetSize| does not return until the pool is
// resized or the pool's |Run| method has errored.
//
// |Func|s are run within an |errgroup|. If any |Func| exists with a non-|nil|
// error, |Run| will exit with an error as well.
type Dynamic struct {
	// errgroup Context in which all spawned |Func|s are run.
	ctx context.Context
	// errgroup in which all spawned |Func|s are run.
	eg *errgroup.Group

	// Guards |size| and |running|.
	mu sync.Mutex

	// |SetSize| does not adjust the thread pool unless the pool is already running.
	running bool

	// The requested size of the pool. After the pool is running, this is
	// also the current size of the pool.
	size int

	// The worker function each thread in the pool runs.
	f Func

	// Sends are made on this channel to request a spawn.
	spawnCh chan struct{}

	// Sends are made on this channel to request a thread to shutdown. This
	// channel is supplied to |Func|s and they are responsible for adhering
	// to the request.
	shutdownCh chan struct{}

	// Sends are made on this channel when the thread successfully exits.
	// When decreasing the number of workers, delivers on |shutdownCh| must
	// be paired up with receives on |exitCh|.
	exitCh chan struct{}

	// Closed when it is time for the pool to shutdown cleanly. Used by |Close|.
	poolShutdownCh chan struct{}
}

func NewDynamic(f Func, size int) *Dynamic {
	if size == 0 {
		panic("cannot create pool of initial size 0")
	}
	if f == nil {
		panic("cannot create pool with nil Func")
	}
	eg, ctx := errgroup.WithContext(context.Background())
	return &Dynamic{
		ctx:  ctx,
		eg:   eg,
		size: size,
		f:    f,

		spawnCh:    make(chan struct{}),
		shutdownCh: make(chan struct{}),
		exitCh:     make(chan struct{}),

		poolShutdownCh: make(chan struct{}),
	}
}

func (d *Dynamic) Run() error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return errors.New("internal error: Dynamic Run() was called on a pool which has already been run or is currently running.")
	}
	d.running = true
	d.eg.Go(func() error {
		for {
			select {
			case <-d.spawnCh:
				d.eg.Go(func() (err error) {
					defer func() {
						if err != nil {
							return
						}
						select {
						case d.exitCh <- struct{}{}:
						case <-d.ctx.Done():
						}
					}()
					return d.f(d.ctx, d.shutdownCh)
				})
			case <-d.ctx.Done():
				return nil
			case <-d.poolShutdownCh:
				return nil
			}
		}
	})
	for i := 0; i < d.size; i++ {
		select {
		case d.spawnCh <- struct{}{}:
		case <-d.ctx.Done():
			d.mu.Unlock()
			return d.eg.Wait()
		}
	}
	d.mu.Unlock()
	return d.eg.Wait()
}

func (d *Dynamic) Close() {
	d.SetSize(0)
	close(d.poolShutdownCh)
}

func (d *Dynamic) SetSize(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.size == 0 {
		// Ignore the request; this pool is shutdown.
		return
	}
	// We are spawning new threads.
	for d.size < n {
		select {
		case d.spawnCh <- struct{}{}:
		case <-d.ctx.Done():
			return
		}
		d.size += 1
	}
	// We are shutting down existing threads.
	for d.size > n {
		select {
		case d.shutdownCh <- struct{}{}:
		case <-d.ctx.Done():
			return
		}
		select {
		case <-d.exitCh:
		case <-d.ctx.Done():
			return
		}
		d.size -= 1
	}
}
