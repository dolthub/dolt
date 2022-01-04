// Copyright 2020 Dolthub, Inc.
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

package index

import (
	"context"
	"fmt"
	"runtime"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/store/types"
)

type lookupResult struct {
	idx uint64
	r   sql.Row
	err error
}

// toLookup represents an table lookup that should be performed by one of the global asyncLookups instance's worker routines
type toLookup struct {
	idx        uint64
	t          types.Tuple
	tupleToRow func(context.Context, types.Tuple) (sql.Row, error)
	resBuf     *async.RingBuffer
	epoch      int
	ctx        context.Context
}

// asyncLookups is a pool of worker routines reading from a channel doing table lookups
type asyncLookups struct {
	ctx        context.Context
	toLookupCh chan toLookup
}

// newAsyncLookups kicks off a number of worker routines and creates a channel for sending lookups to workers.  The
// routines live for the life of the program
func newAsyncLookups(bufferSize int) *asyncLookups {
	toLookupCh := make(chan toLookup, bufferSize)
	art := &asyncLookups{toLookupCh: toLookupCh}

	return art
}

func (art *asyncLookups) start(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go func() {
			art.workerFunc()
		}()
	}
}

func (art *asyncLookups) workerFunc() {
	f := func() {
		var curr toLookup
		var ok bool

		defer func() {
			if r := recover(); r != nil {
				// Attempt to write a failure to the channel and discard any additional errors
				if err, ok := r.(error); ok {
					_ = curr.resBuf.Push(lookupResult{idx: curr.idx, r: nil, err: err}, curr.epoch)
				} else {
					_ = curr.resBuf.Push(lookupResult{idx: curr.idx, r: nil, err: fmt.Errorf("%v", r)}, curr.epoch)
				}
			}

			// if the channel used for lookups is closed then fail spectacularly
			if !ok {
				panic("toLookup channel closed.  All lookups will fail and will result in a deadlock")
			}
		}()

		for {
			curr, ok = <-art.toLookupCh

			if !ok {
				break
			}

			r, err := curr.tupleToRow(curr.ctx, curr.t)
			_ = curr.resBuf.Push(lookupResult{idx: curr.idx, r: r, err: err}, curr.epoch)
		}
	}

	// these routines will run forever unless f is allowed to panic which only happens when the lookup channel is closed
	for {
		f()
	}
}

// lookups is a global asyncLookups instance which is used by the indexLookupRowIterAdapter
var lookups *asyncLookups

func init() {
	lookups = newAsyncLookups(runtime.NumCPU() * 256)
	lookups.start(runtime.NumCPU())
}
