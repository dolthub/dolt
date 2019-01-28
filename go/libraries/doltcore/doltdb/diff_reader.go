package doltdb

import (
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"io"
	"time"
)

const (
	DiffTypeProp    = "difftype"
	CollChangesProp = "collchanges"
)

type DiffChType int

const (
	DiffAdded DiffChType = iota
	DiffRemoved
	DiffModifiedOld
	DiffModifiedNew
)

type DiffTyped interface {
	DiffType() DiffChType
}

type DiffRow struct {
	*table.Row
	diffType DiffChType
}

func (dr *DiffRow) DiffType() DiffChType {
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

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
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
			oldRow := table.NewRow(table.RowDataFromPKAndValueList(rdRd.oldConv.SrcSch, d.KeyValue, d.OldValue.(types.Tuple)))
			mappedOld, _ = rdRd.oldConv.Convert(oldRow)
		}

		if d.NewValue != nil {
			newRow := table.NewRow(table.RowDataFromPKAndValueList(rdRd.newConv.SrcSch, d.KeyValue, d.NewValue.(types.Tuple)))
			mappedNew, _ = rdRd.newConv.Convert(newRow)
		}

		var oldProps = map[string]interface{}{DiffTypeProp: DiffRemoved}
		var newProps = map[string]interface{}{DiffTypeProp: DiffAdded}
		if d.OldValue != nil && d.NewValue != nil {
			oldColDiffs := make(map[string]DiffChType)
			newColDiffs := make(map[string]DiffChType)
			for i := 0; i < rdRd.outSch.NumFields(); i++ {
				oldVal, fld := mappedOld.GetField(i)
				newVal, _ := mappedNew.GetField(i)

				fldName := fld.NameStr()
				inOld := rdRd.oldConv.SrcSch.GetFieldIndex(fldName) != -1
				inNew := rdRd.newConv.SrcSch.GetFieldIndex(fldName) != -1
				if inOld && inNew {
					if !oldVal.Equals(newVal) {
						newColDiffs[fldName] = DiffModifiedNew
						oldColDiffs[fldName] = DiffModifiedOld
					}
				} else if inOld {
					oldColDiffs[fldName] = DiffRemoved
				} else {
					newColDiffs[fldName] = DiffAdded
				}
			}

			oldProps = map[string]interface{}{DiffTypeProp: DiffModifiedOld, CollChangesProp: oldColDiffs}
			newProps = map[string]interface{}{DiffTypeProp: DiffModifiedNew, CollChangesProp: newColDiffs}
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
