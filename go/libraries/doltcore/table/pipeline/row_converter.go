package pipeline

import "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"

func GetRowConvTransformFunc(rc *table.RowConverter) func(*table.Row) ([]*TransformedRowResult, string) {
	return func(inRow *table.Row) (outRows []*TransformedRowResult, badRowDetails string) {
		outData, err := rc.Convert(inRow)

		if err != nil {
			return nil, err.Error()
		}

		return []*TransformedRowResult{{RowData: outData}}, ""
	}
}
