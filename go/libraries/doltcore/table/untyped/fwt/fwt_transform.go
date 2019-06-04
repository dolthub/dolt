package fwt

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
)

// TooLongBehavior determines how the FWTTransformer should behave when it encounters a column that is longer than what
// it expected
type TooLongBehavior int

const (
	// ErrorWhenTooLong treats each row containing a column that is longer than expected as a bad row
	ErrorWhenTooLong TooLongBehavior = iota
	// SkipRowWhenTooLong skips any rows that have columns that are longer than expected
	SkipRowWhenTooLong
	// TruncateWhenTooLong will cut off the end of columns that are too long
	TruncateWhenTooLong
	// HashFillWhenTooLong will result in ######### being printed in place of the columns that are longer than expected.
	HashFillWhenTooLong
	// PrintAllWhenTooLong will print the entire column for every row.  When this happens results will not be valid
	// fixed width text files
	PrintAllWhenTooLong
)

// FWTTransformer transforms columns to be of fixed width.
type FWTTransformer struct {
	fwtSch    *FWTSchema
	colBuffs  map[uint64][]rune
	tooLngBhv TooLongBehavior
}

// NewFWTTransform creates a new FWTTransformer from a FWTSchema and a TooLongBehavior
func NewFWTTransformer(fwtSch *FWTSchema, tooLngBhv TooLongBehavior) *FWTTransformer {
	numFields := fwtSch.Sch.GetAllCols().Size()
	colBuffs := make(map[uint64][]rune, numFields)

	for tag, numRunes := range fwtSch.TagToMaxRunes {
		colBuffs[tag] = make([]rune, numRunes)
	}

	return &FWTTransformer{fwtSch, colBuffs, tooLngBhv}
}

// Transform takes in a row and transforms it so that it's columns are of the correct width.
func (fwtTr *FWTTransformer) Transform(r row.Row, props pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {
	sch := fwtTr.fwtSch.Sch
	destFields := make(row.TaggedValues)

	for tag, colWidth := range fwtTr.fwtSch.TagToWidth {
		buf := fwtTr.colBuffs[tag]

		if colWidth != 0 {
			val, _ := r.GetColVal(tag)

			if types.IsNull(val) {
				// don't assign a value for nil columns
				continue
			}
			str := string(val.(types.String))
			strWidth := StringWidth(str)

			if strWidth > colWidth {
				switch fwtTr.tooLngBhv {
				case ErrorWhenTooLong:
					col, _ := sch.GetAllCols().GetByTag(tag)
					return nil, "Value for " + col.Name + " too long."
				case SkipRowWhenTooLong:
					return nil, ""
				case TruncateWhenTooLong:
					str = str[0:colWidth]
				case HashFillWhenTooLong:
					str = fwtTr.fwtSch.NoFitStrs[tag]
				case PrintAllWhenTooLong:
					break
				}
			}

			strWidth = StringWidth(str)
			if strWidth > colWidth {
				buf = []rune(str)
			} else {
				n := copy(buf, []rune(str))
				// Character widths are tricky. Always overwrite from where we left off to the end of the buffer to clear it.
				for i := 0; n + i < len(buf); i++ {
					buf[n + i] = ' '
				}
			}

		}

		destFields[tag] = types.String(buf)
	}

	r = row.New(sch, destFields)
	return []*pipeline.TransformedRowResult{{RowData: r}}, ""
}
