package table

import (
	"context"
	"math"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
)

// RowIter wraps a sql.RowIter and abstracts way a sql.Context for a
// context.Context.
type RowIter interface {
	Next(ctx context.Context) (sql.Row, error)
	Close(ctx context.Context) error
}

type rowIterImpl struct {
	inner  sql.RowIter
	sqlCtx *sql.Context
}

// NewRowIter returns a RowIter that wraps |inner|. Ctx passed to Next is
// converted to *sql.Context.
func NewRowIter(inner sql.RowIter) RowIter {
	return rowIterImpl{inner: inner}
}

// Next implements RowIter.
func (i rowIterImpl) Next(ctx context.Context) (sql.Row, error) {
	r, err := i.inner.Next(&sql.Context{Context: ctx})
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Close implements RowIter.
func (i rowIterImpl) Close(ctx context.Context) error {
	return i.inner.Close(&sql.Context{Context: ctx})
}

// NewTableIterator creates a RowIter that iterates sql.Row's from |idx|.
// |offset| can be supplied to read at some start point in |idx|.
func NewTableIterator(ctx context.Context, sch schema.Schema, idx durable.Index, offset uint64) (RowIter, error) {
	var rowItr sql.RowIter
	if types.IsFormat_DOLT_1(idx.Format()) {
		m := durable.ProllyMapFromIndex(idx)
		itr, err := m.IterOrdinalRange(ctx, offset, math.MaxUint64)
		if err != nil {
			return nil, err
		}
		s, err := sqlutil.FromDoltSchema("", sch)
		if err != nil {
			return nil, err
		}
		rowItr, err = index.NewProllyRowIter(sch, s.Schema, m, itr, nil)
		if err != nil {
			return nil, err
		}
	} else {

		noms := durable.NomsMapFromIndex(idx)
		itr, err := noms.IteratorAt(ctx, offset)
		if err != nil {
			return nil, err
		}
		conv := MakeNomsConverter(idx.Format(), sch)
		rowItr = index.NewDoltMapIter(itr.NextTuple, nil, conv)
	}
	return NewRowIter(rowItr), nil
}

// MakeNomsConverter creates a *index.KVToSqlRowConverter.
func MakeNomsConverter(nbf *types.NomsBinFormat, sch schema.Schema) *index.KVToSqlRowConverter {
	cols := sch.GetAllCols().GetColumns()
	tagToSqlColIdx := make(map[uint64]int)
	for i, col := range cols {
		tagToSqlColIdx[col.Tag] = i
	}
	return index.NewKVToSqlRowConverter(nbf, tagToSqlColIdx, cols, len(cols))
}
