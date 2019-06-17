package resultset

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

// A table result is a set of rows packaged with their schema.
type TableResult struct {
	Rows   []row.Row
	Schema schema.Schema
}

type RowIterator struct {
	i int
	rows []row.Row
}

// Iterator returns an iterator over this result set
func (tr TableResult) Iterator() *RowIterator {
	return &RowIterator{
		rows:  tr.Rows,
	}
}

// Returns the next row in this result, or nil if there isn't one
func (itr *RowIterator) NextRow() row.Row {
	if itr.i >= len(itr.rows) {
		return nil
	}
	r := itr.rows[itr.i]
	itr.i++
	return r
}
