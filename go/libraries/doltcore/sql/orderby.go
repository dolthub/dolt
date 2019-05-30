package sql

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/xwb1989/sqlparser"
)

// A row sorter knows how to sort rows in a result set using its provided Less function. Init() must be called before
// use.
type RowSorter struct {
	orderBys []*OrderBy
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

		if leftVal.Less(rightVal) {
			return ob.direction.lessVal(true)
		} else if rightVal.Less(leftVal) {
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
	rowValGetter  *RowValGetter
	direction     orderDirection
	InitValue
}

func (ob *OrderBy) Init(resolver TagResolver) error {
	return ob.rowValGetter.Init(resolver)
}

// Processes the order by clause and returns a RowSorter that implements it, or returns an error if it cannot.
func createRowSorter(statement *SelectStatement, orderBy sqlparser.OrderBy) (*RowSorter, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}

	obs := make([]*OrderBy, len(orderBy))
	for i, o := range orderBy {
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

	return &RowSorter{orderBys: obs}, nil
}