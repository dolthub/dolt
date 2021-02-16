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

package sqle

import (
	"fmt"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

type lookupResult struct {
	r   sql.Row
	err error
}

// safeWrite recovers from panics and returns recoverevd objects
func safeWrite(resCh chan lookupResult, result lookupResult) (recovered interface{}) {
	defer func() {
		recovered = recover()
	}()

	resCh <- result
	return
}

// toLookup represents an table lookup that should be performed by one of the global asyncLookups instance's worker routines
type toLookup struct {
	// key read f
	t          types.Tuple
	tupleToRow func(types.Tuple) (sql.Row, error)
	resChan    chan lookupResult
}

// a global asyncLookups struct handles all lookups
type asyncLookups struct {
	toLookupCh chan toLookup
}

func newAsyncLookups(bufferSize int) *asyncLookups {
	toLookupCh := make(chan toLookup, bufferSize)
	art := &asyncLookups{toLookupCh: toLookupCh}
	go art.run()
	return art
}

func (art *asyncLookups) run() {
	f := func() {
		var curr toLookup
		var ok bool

		defer func() {
			if r := recover(); r != nil {
				if curr.resChan != nil {
					// Attempt to write a failure to the channel and discard any additional panics
					if err, ok := r.(error); ok {
						_ = safeWrite(curr.resChan, lookupResult{r: nil, err: err})
					} else {
						_ = safeWrite(curr.resChan, lookupResult{r: nil, err: fmt.Errorf("%v", r)})
					}
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

			r, err := curr.tupleToRow(curr.t)
			curr.resChan <- lookupResult{r: r, err: err}
		}
	}

	// these routines will run forever unless f is allowed to panic which only happens when the lookup channel is closed
	for {
		f()
	}
}

var lookupWorkerPool = &sync.Pool{
	New: func() interface{} {
		return newAsyncLookups(1024)
	},
}
