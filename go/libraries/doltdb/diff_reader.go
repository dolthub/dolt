package doltdb

import (
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"io"
	"time"
)

const (
	DiffTypeProp    = "difftype"
	CollChangesProp = "collchanges"
)

var newRowProps = map[string]interface{}{DiffTypeProp: types.DiffChangeAdded}
var removedRowProps = map[string]interface{}{DiffTypeProp: types.DiffChangeRemoved}

type DiffTyped interface {
	DiffType() types.DiffChangeType
}

type DiffRow struct {
	*table.Row
	diffType types.DiffChangeType
}

func (dr *DiffRow) DiffType() types.DiffChangeType {
	return dr.diffType
}

type RowDiffReader struct {
	oldConv      *table.RowConverter
	newConv      *table.RowConverter
	ad           *AsyncDiffer
	outSch       *schema.Schema
	bufferedRows []*table.Row
}

func NewRowDiffReader(ad *AsyncDiffer, oldConv, newConv *table.RowConverter, outSch *schema.Schema) *RowDiffReader {
	return &RowDiffReader{
		oldConv,
		newConv,
		ad,
		outSch,
		make([]*table.Row, 0, 1024),
	}
}

// GetSchema gets the schema of the rows that this reader will return
func (rdRd *RowDiffReader) GetSchema() *schema.Schema {
	return rdRd.outSch
}

// ReadRow reads a row from a table.  If there is a bad row ErrBadRow will be returned. This is a potentially
// non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (rdRd *RowDiffReader) ReadRow() (*table.Row, error) {
	if len(rdRd.bufferedRows) != 0 {
		return rdRd.nextFromBuffer(), nil
	}

	if rdRd.ad.isDone {
		return nil, io.EOF
	}

	diffs := rdRd.ad.GetDiffs(1, time.Second)

	if len(diffs) == 0 {
		if rdRd.ad.isDone {
			return nil, io.EOF
		}

		return nil, errors.New("timeout")
	}

	for _, d := range diffs {
		var mappedOld *table.RowData
		var mappedNew *table.RowData

		if d.OldValue != nil {
			oldRow := table.NewRow(table.RowDataFromPKAndValueList(rdRd.oldConv.SrcSch, d.KeyValue, d.OldValue.(types.List)))
			mappedOld, _ = rdRd.oldConv.Convert(oldRow)
		}

		if d.NewValue != nil {
			newRow := table.NewRow(table.RowDataFromPKAndValueList(rdRd.newConv.SrcSch, d.KeyValue, d.NewValue.(types.List)))
			mappedNew, _ = rdRd.newConv.Convert(newRow)
		}

		oldProps := removedRowProps
		newProps := newRowProps
		if d.OldValue != nil && d.NewValue != nil {
			oldColDiffs := make(map[string]types.DiffChangeType)
			newColDiffs := make(map[string]types.DiffChangeType)
			for i := 0; i < rdRd.outSch.NumFields(); i++ {
				oldVal, fld := mappedOld.GetField(i)
				newVal, _ := mappedNew.GetField(i)

				fldName := fld.NameStr()
				inOld := rdRd.oldConv.SrcSch.GetFieldIndex(fldName) != -1
				inNew := rdRd.newConv.SrcSch.GetFieldIndex(fldName) != -1
				if inOld && inNew {
					if !oldVal.Equals(newVal) {
						newColDiffs[fldName] = types.DiffChangeModified
						oldColDiffs[fldName] = types.DiffChangeModified
					}
				} else if inOld {
					oldColDiffs[fldName] = types.DiffChangeRemoved
				} else {
					newColDiffs[fldName] = types.DiffChangeAdded
				}
			}

			oldProps = map[string]interface{}{DiffTypeProp: types.DiffChangeModified, CollChangesProp: oldColDiffs}
			newProps = map[string]interface{}{DiffTypeProp: types.DiffChangeModified, CollChangesProp: newColDiffs}
		}

		if d.OldValue != nil {
			rdRd.bufferedRows = append(rdRd.bufferedRows, table.NewRowWithProperties(mappedOld, oldProps))
		}

		if d.NewValue != nil {
			rdRd.bufferedRows = append(rdRd.bufferedRows, table.NewRowWithProperties(mappedNew, newProps))
		}
	}

	return rdRd.nextFromBuffer(), nil
}

func (rdRd *RowDiffReader) nextFromBuffer() *table.Row {
	r := rdRd.bufferedRows[0]
	rdRd.bufferedRows = rdRd.bufferedRows[1:]

	return r
}

// Close should release resources being held
func (rdRd *RowDiffReader) Close() error {
	rdRd.ad.Close()
	return nil
}
