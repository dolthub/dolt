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

package sql

import (
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/store/types"

	"sort"
)

// A row sorter knows how to sort rows in a result set using its provided Less function. Init() must be called before
// use.
type RowSorter struct {
	orderBys []*OrderBy
	nbf      *types.NomsBinFormat
	InitValue
}

func (rs *RowSorter) Init(resolver TagResolver) error {
	if rs == nil {
		return nil
	}

	for _, ob := range rs.orderBys {
		if err := ob.Init(resolver); err != nil {
			return err
		}
	}
	return nil
}

// Less returns whether rLeft < rRight.
// Init() must be called before use.
func (rs *RowSorter) Less(rLeft, rRight row.Row) bool {
	for _, ob := range rs.orderBys {
		leftVal := ob.rowValGetter.Get(rLeft)
		rightVal := ob.rowValGetter.Get(rRight)

		// MySQL behavior is that nulls sort first in asc, last in desc
		if types.IsNull(leftVal) {
			return ob.direction.lessVal(true)
		} else if types.IsNull(rightVal) {
			return ob.direction.lessVal(false)
		}

		if leftVal.Less(rs.nbf, rightVal) {
			return ob.direction.lessVal(true)
		} else if rightVal.Less(rs.nbf, leftVal) {
			return ob.direction.lessVal(false)
		}
	}

	return false
}

type orderDirection bool

const (
	asc  orderDirection = true
	desc orderDirection = false
)

// Returns the appropriate less value for sorting, reversing the sort order for desc orders.
func (od orderDirection) lessVal(less bool) bool {
	switch od {
	case asc:
		return less
	case desc:
		return !less
	}
	panic("impossible")
}

// OrderBy represents a single order-by clause of potentially many in a query
type OrderBy struct {
	rowValGetter *RowValGetter
	direction    orderDirection
	InitValue
}

func (ob *OrderBy) Init(resolver TagResolver) error {
	return ob.rowValGetter.Init(resolver)
}

// Processes the order by clause and returns a RowSorter that implements it, or returns an error if it cannot.
func createRowSorter(nbf *types.NomsBinFormat, statement *SelectStatement, orderBy sqlparser.OrderBy) (*RowSorter, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}

	obs := make([]*OrderBy, len(orderBy))
	for i, o := range orderBy {
		// first to see if the order by expression is one of the selected column aliases

		getter, err := getterFor(o.Expr, statement.inputSchemas, statement.aliases)
		if err != nil {
			return nil, err
		}

		dir := asc
		if o.Direction == sqlparser.DescScr {
			dir = desc
		}

		obs[i] = &OrderBy{rowValGetter: getter, direction: dir}
	}

	return &RowSorter{orderBys: obs, nbf: nbf}, nil
}

// Boolean lesser function for rows. Returns whether rLeft < rRight
type rowLesserFn func(rLeft row.Row, rRight row.Row) bool

// Returns a sorting transform for the rowLesserFn given. The transform will necessarily block until it receives all
// input rows before sending rows to the rest of the pipeline.
func newSortingTransform(lesser rowLesserFn) pipeline.TransformFunc {
	rows := make([]pipeline.RowWithProps, 0)

	sortAndWrite := func(outChan chan<- pipeline.RowWithProps) {
		sort.Slice(rows, func(i, j int) bool {
			return lesser(rows[i].Row, rows[j].Row)
		})
		for _, r := range rows {
			outChan <- r
		}
	}

	return func(inChan <-chan pipeline.RowWithProps, outChan chan<- pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure, stopChan <-chan struct{}) {
		for {
			select {
			case <-stopChan:
				sortAndWrite(outChan)
				return
			default:
			}

			select {
			case r, ok := <-inChan:
				if ok {
					rows = append(rows, r)
				} else {
					sortAndWrite(outChan)
					return
				}

			case <-stopChan:
				sortAndWrite(outChan)
				return
			}
		}
	}
}
