package rowconv

import (
	"fmt"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

var IdentityConverter = &RowConverter{nil, true, nil}

// RowConverter converts rows from one schema to another
type RowConverter struct {
	// FieldMapping is a mapping from source column to destination column
	*FieldMapping
	// IdentityConverter is a bool which is true if the converter is doing nothing.
	IdentityConverter bool
	ConvFuncs         map[uint64]doltcore.ConvFunc
}

func newIdentityConverter(mapping *FieldMapping) *RowConverter {
	return &RowConverter{mapping, true, nil}
}

// NewRowConverter creates a a row converter from a given FieldMapping.
func NewRowConverter(mapping *FieldMapping) (*RowConverter, error) {
	if !isNecessary(mapping.SrcSch, mapping.DestSch, mapping.SrcToDest) {
		return newIdentityConverter(mapping), nil
	}

	convFuncs := make(map[uint64]doltcore.ConvFunc, len(mapping.SrcToDest))
	for srcTag, destTag := range mapping.SrcToDest {
		destCol, destOk := mapping.DestSch.GetAllCols().GetByTag(destTag)
		srcCol, srcOk := mapping.SrcSch.GetAllCols().GetByTag(srcTag)

		if !destOk || !srcOk {
			return nil, fmt.Errorf("Could not find column being mapped. src tag: %d, dest tag: %d", srcTag, destTag)
		}

		convFuncs[srcTag] = doltcore.GetConvFunc(srcCol.Kind, destCol.Kind)

		if convFuncs[srcTag] == nil {
			return nil, fmt.Errorf("Unsupported conversion from type %s to %s", srcCol.KindString(), destCol.KindString())
		}
	}

	return &RowConverter{mapping, false, convFuncs}, nil
}

// Convert takes a row maps its columns to their destination columns, and performs any type conversion needed to create
// a row of the expected destination schema.
func (rc *RowConverter) Convert(inRow row.Row) (row.Row, error) {
	if rc.IdentityConverter {
		return inRow, nil
	}

	outTaggedVals := make(row.TaggedValues, len(rc.SrcToDest))
	err := pantoerr.PanicToError("error converting row", func() error {
		var convErr error
		inRow.IterCols(func(tag uint64, val types.Value) (stop bool) {
			convFunc, ok := rc.ConvFuncs[tag]

			if ok {
				outTag := rc.SrcToDest[tag]
				outVal, err := convFunc(val)

				if err != nil {
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

	outRow := row.New(inRow.Format(), rc.DestSch, outTaggedVals)

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
	destPKCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		srcPKCol := srcPKCols.GetByIndex(i)

		if srcPKCol.Tag != col.Tag {
			return true
		}

		i++
		return false
	})

	return false
}

// GetRowConvTranformFunc can be used to wrap a RowConverter and use that RowConverter in a pipeline.
func GetRowConvTransformFunc(rc *RowConverter) func(row.Row, pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {
	if rc.IdentityConverter {
		return func(inRow row.Row, props pipeline.ReadableMap) (outRows []*pipeline.TransformedRowResult, badRowDetails string) {
			return []*pipeline.TransformedRowResult{{inRow, nil}}, ""
		}
	} else {
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
}
