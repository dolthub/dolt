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

package querydiff

import (
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/dolt/go/store/atomicerr"
)

const (
	bufRowIterSize = 1024
)

type iterQueue struct {
	ctx         *sql.Context
	currRow     sql.Row
	iter        sql.RowIter
	rowChan     chan sql.Row
	projections []sql.Expression
	ae          *atomicerr.AtomicError
}

func newIterQueue(ctx *sql.Context, iter sql.RowIter, projections []sql.Expression, ae *atomicerr.AtomicError) *iterQueue {
	return &iterQueue{
		ctx:         ctx,
		iter:        iter,
		rowChan:     make(chan sql.Row, bufRowIterSize),
		projections: projections,
		ae:          ae,
	}
}

func (iq *iterQueue) start() {
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
}

func (iq *iterQueue) peek() sql.Row {
	return iq.currRow
}

func (iq *iterQueue) pop() sql.Row {
	r := iq.currRow
	iq.currRow = <-iq.rowChan
	return r
}

func (iq *iterQueue) projectPop() (r sql.Row, err error) {
	r = iq.pop()
	if r != nil && iq.projections != nil {
		r, err = plan.ProjectRow(iq.ctx, iq.projections, r)
	}
	return r, err
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
