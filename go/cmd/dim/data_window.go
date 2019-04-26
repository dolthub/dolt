package main

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"log"
	"runtime/debug"
)

type DataWindow struct {
	data types.Map
	itr  types.MapIterator

	dimRows    []*DimRow
	changedSet map[int]struct{}

	toUntyped *rowconv.RowConverter
	toTyped   *rowconv.RowConverter

	idx  int
	size int
}

func appendRow(drs []*DimRow, toUntyped, toTyped *rowconv.RowConverter, k, v types.Value) ([]*DimRow, bool) {
	if !types.IsNull(k) && !types.IsNull(v) {
		r := row.FromNoms(toUntyped.SrcSch, k.(types.Tuple), v.(types.Tuple))
		dr, err := NewDimRow(r, toUntyped, toTyped)

		if err != nil {
			panic(err)
		}

		return append(drs, dr), true
	}

	return drs, false
}

func NewDataWindow(size int, data types.Map, toUntyped, toTyped *rowconv.RowConverter) *DataWindow {
	itr := data.Iterator(context.TODO())

	ok := true
	var drs []*DimRow
	for i := 0; i < size && ok; i++ {
		k, v := itr.Next(context.TODO())
		drs, ok = appendRow(drs, toUntyped, toTyped, k, v)
	}

	log.Println("data in window", len(drs))

	return &DataWindow{
		data,
		itr,
		drs,
		make(map[int]struct{}),
		toUntyped,
		toTyped,
		0,
		size,
	}
}

func (dw *DataWindow) Resize(size, selRow int) int {
	shrunk := size < dw.size

	dw.size = size

	if !shrunk {
		dw.fillInData()
		return selRow
	} else {
		absSel := dw.idx + selRow
		dw.idx = absSel - dw.size/2

		if dw.idx < 0 {
			dw.idx = 0
		}

		return absSel - dw.idx
	}
}

func (dw *DataWindow) Size() int {
	return dw.size
}

func (dw *DataWindow) CanMoveUp() bool {
	return dw.idx != 0
}

func (dw *DataWindow) CanMoveDown() bool {
	return uint64(dw.idx+dw.size) < dw.data.Len()
}

func (dw *DataWindow) MoveUp() {
	if dw.idx > 0 {
		dw.idx--
	}
}

func (dw *DataWindow) PageUp() {
	dw.idx -= dw.size - 1

	if dw.idx < 0 {
		dw.idx = 0
	}
}

func (dw *DataWindow) fillInData() {
	for len(dw.dimRows) < dw.idx+dw.size {
		k, v := dw.itr.Next(context.TODO())

		var ok bool
		dw.dimRows, ok = appendRow(dw.dimRows, dw.toUntyped, dw.toTyped, k, v)

		if !ok {
			break
		}
	}

	if dw.idx >= len(dw.dimRows) {
		dw.idx = len(dw.dimRows) - 1
	}
}

func (dw *DataWindow) MoveDown() {
	dw.idx += 1
	dw.fillInData()
}

func (dw *DataWindow) PageDown() {
	dw.idx += dw.size - 1
	dw.fillInData()
}

func (dw *DataWindow) IterWindow(cb func(*DimRow)) {
	for i := 0; i < dw.size; i++ {
		absIdx := dw.idx + i

		if absIdx >= 0 && absIdx < len(dw.dimRows) {
			cb(dw.dimRows[absIdx])
		}
	}
}

func (dw *DataWindow) NthVisibleRow(n int) *DimRow {
	absIdx := dw.idx + n

	if absIdx >= 0 && absIdx < len(dw.dimRows) {
		return dw.dimRows[absIdx]
	}

	return nil
}

func (dw *DataWindow) UpdateRow(n int) {
	absIdx := dw.idx + n
	dw.changedSet[absIdx] = struct{}{}
}

func (dw *DataWindow) FlushEdits() {
	defer func() {
		if r := recover(); r != nil {
			log.Println("recovered from:", r)
			log.Println(string(debug.Stack()))
		}
	}()

	if dw.HasEdits() {
		me := dw.data.Edit()

		for idx := range dw.changedSet {
			dr := dw.dimRows[idx]
			me = dr.StoreValue(me)
		}

		log.Println("flushed edits")
		dw.data = me.Map(context.TODO())
	}
}

func (dw *DataWindow) HasEdits() bool {
	log.Println("num changes", len(dw.changedSet))
	return len(dw.changedSet) > 0
}
