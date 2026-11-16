// Copyright 2026 Dolthub, Inc.
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

package val

import (
	"bytes"
	"context"

	"github.com/dolthub/go-mysql-server/sql"
)

// OrderedTupleComparator compares each field in the sort order it was defined with, wrapping a comparator that orders
// every field ascending with NULLs first.
type OrderedTupleComparator struct {
	innerCmp TupleComparator
	orders   []sql.IndexColumnOrder
}

var _ TupleComparator = (*OrderedTupleComparator)(nil)

// Compare implements the TupleComparator interface.
func (c *OrderedTupleComparator) Compare(ctx context.Context, left, right Tuple, desc *TupleDesc) (cmp int, err error) {
	fast := desc.GetFixedAccess()
	off := len(fast)
	var start, stop ByteSize
	for i := 0; i < off; i++ {
		stop = fast[i]
		cmp, err = c.CompareValues(ctx, i, left[start:stop], right[start:stop], desc.Types[i])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
		start = stop
	}

	for i, typ := range desc.Types[off:] {
		j := i + off
		cmp, err = c.CompareValues(ctx, j, left.GetField(j), right.GetField(j), typ)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return
}

// CompareValues implements the TupleComparator interface.
func (c *OrderedTupleComparator) CompareValues(ctx context.Context, index int, left, right []byte, typ Type) (int, error) {
	order := c.orders[index]
	if !order.Descending && !order.NullsLast {
		return c.innerCmp.CompareValues(ctx, index, left, right, typ)
	}
	if left == nil || right == nil {
		if bytes.Equal(left, right) {
			return 0, nil
		}
		cmp := 1
		if left == nil {
			cmp = -1
		}
		if order.NullsLast {
			cmp = -cmp
		}
		return cmp, nil
	}
	cmp, err := c.innerCmp.CompareValues(ctx, index, left, right, typ)
	if order.Descending {
		cmp = -cmp
	}
	return cmp, err
}

// Prefix implements the TupleComparator interface.
func (c *OrderedTupleComparator) Prefix(n int) TupleComparator {
	return &OrderedTupleComparator{innerCmp: c.innerCmp.Prefix(n), orders: c.orders[:n]}
}

// Suffix implements the TupleComparator interface.
func (c *OrderedTupleComparator) Suffix(n int) TupleComparator {
	return &OrderedTupleComparator{innerCmp: c.innerCmp.Suffix(n), orders: c.orders[len(c.orders)-n:]}
}

// Validated implements the TupleComparator interface. Fields past the end of the defined orders, such as a trailing
// hash field, take the default order.
func (c *OrderedTupleComparator) Validated(types []Type) TupleComparator {
	orders := make([]sql.IndexColumnOrder, len(types))
	copy(orders, c.orders)
	return &OrderedTupleComparator{innerCmp: c.innerCmp.Validated(types), orders: orders}
}

// WithValueStore implements the TupleComparator interface.
func (c *OrderedTupleComparator) WithValueStore(vs ValueStore) TupleComparator {
	return &OrderedTupleComparator{innerCmp: c.innerCmp.WithValueStore(vs), orders: c.orders}
}

// Order implements the TupleComparator interface.
func (c *OrderedTupleComparator) Order(i int) sql.IndexColumnOrder {
	return c.orders[i]
}
