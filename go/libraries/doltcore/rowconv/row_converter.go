package rowconv

import (
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
)

var IdentityConverter = &RowConverter{nil, true, nil}

type RowConverter struct {
	*FieldMapping
	IdentityConverter bool
	convFuncs         map[uint64]doltcore.ConvFunc
}

func NewRowConverter(mapping *FieldMapping) (*RowConverter, error) {
	if !isNecessary(mapping.SrcSch, mapping.DestSch, mapping.SrcToDest) {
		return IdentityConverter, nil
	}

	convFuncs := make(map[uint64]doltcore.ConvFunc, len(mapping.SrcToDest))
	for srcTag, destTag := range mapping.SrcToDest {
		destCol, destOk := mapping.DestSch.GetAllCols().GetByTag(destTag)
		srcCol, srcOk := mapping.SrcSch.GetAllCols().GetByTag(srcTag)

		if !destOk || !srcOk {
			return nil, fmt.Errorf("Colud not find column being mapped. src tag: %d, dest tag: %d", srcTag, destTag)
		}

		convFuncs[srcTag] = doltcore.GetConvFunc(srcCol.Kind, destCol.Kind)

		if convFuncs[srcTag] == nil {
			return nil, fmt.Errorf("Unsupported conversion from type %s to %s", srcCol.KindString(), destCol.KindString())
		}
	}

	return &RowConverter{mapping, false, convFuncs}, nil
}

func (rc *RowConverter) Convert(inRow row.Row) (row.Row, error) {
	if rc.IdentityConverter {
		return inRow, nil
	}

	outTaggedVals := make(row.TaggedValues, len(rc.SrcToDest))
	err := pantoerr.PanicToError("error converting row", func() error {
		var convErr error
		inRow.IterCols(func(tag uint64, val types.Value) (stop bool) {
			convFunc, ok := rc.convFuncs[tag]

			if ok {
				outTag := rc.SrcToDest[tag]
				outVal, err := convFunc(val)

				if err != nil {
					fmt.Println()
					fmt.Println(types.EncodedValue(val))
					convErr = err
					return true
				}

				outTaggedVals[outTag] = outVal
			}

			return false
		})

		return convErr
	})

	if err != nil {
		return nil, err
	}

	outRow := row.New(rc.DestSch, outTaggedVals)

	return outRow, nil
}

func isNecessary(srcSch, destSch schema.Schema, destToSrc map[uint64]uint64) bool {
	srcCols := srcSch.GetAllCols()
	destCols := destSch.GetAllCols()

	if len(destToSrc) != srcCols.Size() || len(destToSrc) != destCols.Size() {
		return true
	}

	for k, v := range destToSrc {
		if k != v {
			return true
		}

		srcCol, srcOk := srcCols.GetByTag(v)
		destCol, destOk := destCols.GetByTag(k)

		if !srcOk || !destOk {
			panic("There is a bug.  FieldMapping creation should prevent this from happening")
		}

		if srcCol.IsPartOfPK != destCol.IsPartOfPK {
			return true
		}

		if srcCol.Kind != destCol.Kind {
			return true
		}
	}

	srcPKCols := srcSch.GetPKCols()
	destPKCols := destSch.GetPKCols()

	if srcPKCols.Size() != destPKCols.Size() {
		return true
	}

	i := 0
	destPKCols.ItrUnsorted(func(tag uint64, col schema.Column) (stop bool) {
		srcPKCol := srcPKCols.GetByUnsortedIndex(i)

		if srcPKCol.Tag != col.Tag {
			return true
		}

		i++
		return false
	})

	return false
}

func GetRowConvTransformFunc(rc *RowConverter) func(row.Row, pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {
	return func(inRow row.Row, props pipeline.ReadableMap) (outRows []*pipeline.TransformedRowResult, badRowDetails string) {
		outRow, err := rc.Convert(inRow)

		if err != nil {
			return nil, err.Error()
		}

		if !row.IsValid(outRow, rc.DestSch) {
			col := row.GetInvalidCol(outRow, rc.DestSch)
			return nil, "invalid column: " + col.Name
		}

		return []*pipeline.TransformedRowResult{{outRow, nil}}, ""
	}
}
