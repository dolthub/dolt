// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed toIter in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querydiff

import (
	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"io"
)

const (
	bufRowIterSize = 1024
)

type iterQueue struct {
	currRow sql.Row
	iter    sql.RowIter
	rowChan chan sql.Row
	started bool
	ae      *atomicerr.AtomicError
}

func newIterQueue(iter sql.RowIter, ae *atomicerr.AtomicError) *iterQueue {
	return &iterQueue{
		iter:    iter,
		rowChan: make(chan sql.Row, bufRowIterSize),
		ae:      ae,
	}
}

func (iq *iterQueue) maybeStart() {
	if iq.started {
		return
	}

	go func() {
		defer close(iq.rowChan)
		for {
			r, err := iq.iter.Next()
			if r != nil {
				iq.rowChan <- r
			}
			if err != nil {
				if err != io.EOF {
					iq.ae.SetIfError(err)
				}
				break
			}
		}
	}()
	iq.currRow = <-iq.rowChan
	iq.started = true
}

func (iq *iterQueue) peek() sql.Row {
	return iq.currRow
}

func (iq *iterQueue) pop() sql.Row {
	r := iq.currRow
	iq.currRow = <-iq.rowChan
	return r
}

func (iq *iterQueue) isDone() bool {
	return iq.peek() == nil
}

func (iq *iterQueue) close() {
	iq.ae.SetIfError(iq.iter.Close())
	open := true
	for open {
		_, open = <-iq.rowChan
	}
}
