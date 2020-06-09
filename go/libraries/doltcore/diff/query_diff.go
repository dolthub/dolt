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

package diff

import (
	"fmt"
	"io"

	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/store/atomicerr"
)

const (
	bufRowIterSize = 1024
)

type QueryDiffer struct {
	sqlCtx *sql.Context
	from   *rowIterQueue
	to     *rowIterQueue
	rowCmp sql.RowCompareFunc
	sch    sql.Schema
	Joiner *rowconv.Joiner
	ae     *atomicerr.AtomicError
}

func NewQueryDiffer(ctx *sql.Context, from, to sql.OrderableRowIter, cmp sql.RowCompareFunc, sch sql.Schema, joiner *rowconv.Joiner) *QueryDiffer {
	ae := atomicerr.New()
	return &QueryDiffer{
		sqlCtx: ctx,
		from:   &rowIterQueue{iter: from, ae: ae},
		to:     &rowIterQueue{iter: to, ae: ae},
		rowCmp: cmp,
		sch:    sch,
		Joiner: joiner,
		ae:     ae,
	}
}

func (qd *QueryDiffer) Start() {
	qd.from.start()
	qd.to.start()
}

func (qd *QueryDiffer) NextDiff() (sql.Row, sql.Row, error) {
	var err error
	for {
		if qd.ae.IsSet() {
			return nil, nil, qd.ae.Get()
		}
		if qd.from.isDone() && qd.to.isDone() {
			return nil, nil, io.EOF
		}
		if qd.from.isDone() || qd.to.isDone() {
			return qd.from.pop(), qd.to.pop(), nil
		}
		var cmp int
		cmp, err = qd.rowCmp(qd.sqlCtx, qd.from.peek(), qd.to.peek())
		if err != nil {
			return nil, nil, err
		}
		switch cmp {
		case -1:
			return qd.from.pop(), nil, nil
		case 1:
			return nil, qd.to.pop(), nil
		case 0:
			var eq bool
			eq, err = qd.from.peek().Equals(qd.to.peek(), qd.sch)
			if err != nil {
				return nil, nil, err
			}
			if !eq {
				// todo: we don't have any way to detect updates at this point
				// if rows are ordered equally, but not equal in value, consider it a drop/add
				return qd.from.pop(), nil, nil
			} else {
				_ = qd.from.pop()
				_ = qd.to.pop()
				continue
			}
		default:
			panic(fmt.Sprintf("rowCmp() returned incorrect value in QueryDiffer: %d", cmp))
		}
	}
}

func (qd *QueryDiffer) Close() (err error) {
	qd.from.drain()
	qd.to.drain()
	return qd.ae.Get()
}

type rowIterQueue struct {
	currRow sql.Row
	iter    sql.RowIter
	rowChan chan sql.Row
	ae      *atomicerr.AtomicError
}

func (iq *rowIterQueue) start() {
	if iq.iter == nil {
		panic("buffered row iterator does not have child iterator")
	}
	iq.rowChan = make(chan sql.Row, bufRowIterSize)
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
	iq.advance()
}

func (iq *rowIterQueue) peek() sql.Row {
	return iq.currRow
}

func (iq *rowIterQueue) pop() sql.Row {
	r := iq.currRow
	iq.advance()
	return r
}

func (iq *rowIterQueue) advance() {
	r, ok := <-iq.rowChan
	if ok {
		iq.currRow = r
	} else {
		iq.currRow = nil
	}
}

func (iq *rowIterQueue) isDone() bool {
	return iq.peek() == nil
}

func (iq *rowIterQueue) drain() {
	open := true
	for open {
		_, open = <-iq.rowChan
	}
}
