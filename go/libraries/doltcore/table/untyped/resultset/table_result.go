// Copyright 2019 Liquidata, Inc.
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

package resultset

import (
	"sync"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

// A table result is a set of rows packaged with their schema. Rows are filled in concurrently from a channel.
type TableResult struct {
	// The schema of this table
	Schema schema.Schema
	// The rows in this result, filled in concurrently
	rows []row.Row
	// Whether the rows have been finalized
	done bool
	// The number of rows stored so far
	length int
	// A mutex to synchronize logic when iterating over rows that are in the process of being populated.
	mutex sync.Mutex
}

type RowIterator struct {
	i  int
	tr *TableResult
}

// Creates a new TableResult that will consume the channel given and returns it.
func NewTableResult(in chan row.Row, sch schema.Schema) *TableResult {
	tr := &TableResult{Schema: sch}
	tr.consume(in)
	return tr
}

// Creates a pre-canned table result for use in testing.
func newTableResultForTest(rs []row.Row, sch schema.Schema) *TableResult {
	return &TableResult{rows: rs, Schema: sch, done: true, length: len(rs)}
}

// Starts a goroutine to consume the table result's input channel and cache the result in the Rows field.
func (tr *TableResult) consume(in chan row.Row) {
	go func() {
		for {
			tr.mutex.Lock()
			r, ok := <-in
			if ok {
				tr.rows = append(tr.rows, r)
				tr.length++
				tr.mutex.Unlock()
			} else {
				tr.done = true
				tr.mutex.Unlock()
				return
			}
		}
	}()
}

// Iterator returns an iterator over this result set
func (tr *TableResult) Iterator() *RowIterator {
	return &RowIterator{
		tr: tr,
	}
}

// Returns the next row in this result, or nil if there isn't one
func (itr *RowIterator) NextRow() row.Row {
	if !itr.waitForResult() {
		return nil
	}

	itr.tr.mutex.Lock()
	defer itr.tr.mutex.Unlock()

	r := itr.tr.rows[itr.i]
	itr.i++
	return r
}

// waitForResult polls the table result for its next result, blocking until it's ready. Returns whether there is a next
// result to return.
func (itr *RowIterator) waitForResult() bool {
	defer itr.tr.mutex.Unlock()
	for {
		itr.tr.mutex.Lock()
		if itr.i >= itr.tr.length {
			if itr.tr.done {
				return false
			}
		} else {
			return true
		}
		itr.tr.mutex.Unlock()
	}
}
