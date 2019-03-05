package row

import (
	"bytes"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

type RowFormatFunc func(r Row, sch schema.Schema) string

var Fmt = FieldSeparatedFmt(':')
var fieldDelim = []byte(" | ")

func FieldSeparatedFmt(delim rune) RowFormatFunc {
	return func(r Row, sch schema.Schema) string {
		if r == nil {
			return "null"
		}

		allCols := sch.GetAllCols()
		kvps := make([]string, 0, allCols.Size())

		var backingBuffer [512]byte
		buf := bytes.NewBuffer(backingBuffer[:0])

		var ok bool
		allCols.IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
			if ok {
				buf.Write(fieldDelim)
			}

			var val types.Value
			val, ok = r.GetColVal(tag)

			if ok {
				buf.Write([]byte(col.Name))
				buf.WriteRune(delim)
				types.WriteEncodedValue(buf, val)
				kvps = append(kvps, buf.String())
			}

			return false
		})

		return buf.String()
	}
}
