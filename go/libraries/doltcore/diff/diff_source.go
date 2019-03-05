package diff

import (
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/valutil"
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
	row.Row
	diffType DiffChType
}

func (dr *DiffRow) DiffType() DiffChType {
	return dr.diffType
}

type RowDiffSource struct {
	oldConv      *rowconv.RowConverter
	newConv      *rowconv.RowConverter
	ad           *AsyncDiffer
	outSch       schema.Schema
	bufferedRows []pipeline.RowWithProps
}

func NewRowDiffSource(ad *AsyncDiffer, oldConv, newConv *rowconv.RowConverter, outSch schema.Schema) *RowDiffSource {
	return &RowDiffSource{
		oldConv,
		newConv,
		ad,
		outSch,
		make([]pipeline.RowWithProps, 0, 1024),
	}
}

// GetSchema gets the schema of the rows that this reader will return
func (rdRd *RowDiffSource) GetSchema() schema.Schema {
	return rdRd.outSch
}

// NextDiff reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (rdRd *RowDiffSource) NextDiff() (row.Row, pipeline.ImmutableProperties, error) {
	if len(rdRd.bufferedRows) != 0 {
		rowWithProps := rdRd.nextFromBuffer()
		return rowWithProps.Row, rowWithProps.Props, nil
	}

	if rdRd.ad.isDone {
		return nil, pipeline.NoProps, io.EOF
	}

	diffs := rdRd.ad.GetDiffs(1, time.Second)

	if len(diffs) == 0 {
		if rdRd.ad.isDone {
			return nil, pipeline.NoProps, io.EOF
		}

		return nil, pipeline.NoProps, errors.New("timeout")
	}

	outCols := rdRd.outSch.GetAllCols()
	for _, d := range diffs {
		var mappedOld row.Row
		var mappedNew row.Row

		if d.OldValue != nil {
			oldRow := row.FromNoms(rdRd.oldConv.SrcSch, d.KeyValue.(types.Tuple), d.OldValue.(types.Tuple))
			mappedOld, _ = rdRd.oldConv.Convert(oldRow)
		}

		if d.NewValue != nil {
			newRow := row.FromNoms(rdRd.newConv.SrcSch, d.KeyValue.(types.Tuple), d.NewValue.(types.Tuple))
			mappedNew, _ = rdRd.newConv.Convert(newRow)
		}

		var oldProps = map[string]interface{}{DiffTypeProp: DiffRemoved}
		var newProps = map[string]interface{}{DiffTypeProp: DiffAdded}
		if d.OldValue != nil && d.NewValue != nil {
			oldColDiffs := make(map[string]DiffChType)
			newColDiffs := make(map[string]DiffChType)
			outCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
				oldVal, _ := mappedOld.GetColVal(tag)
				newVal, _ := mappedNew.GetColVal(tag)

				_, inOld := rdRd.oldConv.SrcSch.GetAllCols().GetByTag(tag)
				_, inNew := rdRd.newConv.SrcSch.GetAllCols().GetByTag(tag)

				if inOld && inNew {
					if !valutil.NilSafeEqCheck(oldVal, newVal) {
						newColDiffs[col.Name] = DiffModifiedNew
						oldColDiffs[col.Name] = DiffModifiedOld
					}
				} else if inOld {
					oldColDiffs[col.Name] = DiffRemoved
				} else {
					newColDiffs[col.Name] = DiffAdded
				}

				return false
			})

			oldProps = map[string]interface{}{DiffTypeProp: DiffModifiedOld, CollChangesProp: oldColDiffs}
			newProps = map[string]interface{}{DiffTypeProp: DiffModifiedNew, CollChangesProp: newColDiffs}
		}

		if d.OldValue != nil {
			rwp := pipeline.NewRowWithProps(mappedOld, oldProps)
			rdRd.bufferedRows = append(rdRd.bufferedRows, rwp)
		}

		if d.NewValue != nil {
			rwp := pipeline.NewRowWithProps(mappedNew, newProps)
			rdRd.bufferedRows = append(rdRd.bufferedRows, rwp)
		}
	}

	rwp := rdRd.nextFromBuffer()
	return rwp.Row, rwp.Props, nil
}

func (rdRd *RowDiffSource) nextFromBuffer() pipeline.RowWithProps {
	r := rdRd.bufferedRows[0]
	rdRd.bufferedRows = rdRd.bufferedRows[1:]

	return r
}

// Close should release resources being held
func (rdRd *RowDiffSource) Close() error {
	rdRd.ad.Close()
	return nil
}
