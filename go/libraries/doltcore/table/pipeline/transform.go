package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
)

type TransformedRowResult struct {
	RowData    *table.RowData
	Properties map[string]interface{}
}

type TransFailCallback func(row *table.Row, errDetails string)
type TransformFunc func(inChan <-chan *table.Row, outChan chan<- *table.Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool)
type BulkTransformFunc func(rows []*table.Row, outChan chan<- *table.Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool)
type TransformRowFunc func(inRow *table.Row) (rowData []*TransformedRowResult, badRowDetails string)

func NewBulkTransformer(bulkTransFunc BulkTransformFunc) TransformFunc {
	return func(inChan <-chan *table.Row, outChan chan<- *table.Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool) {
		var rows []*table.Row
	RowLoop:
		for {
			select {
			case <-stopChan:
				return
			default:
			}

			select {
			case row, ok := <-inChan:
				if ok {
					rows = append(rows, row)
					break RowLoop
				} else {
					return
				}

			case <-stopChan:
				return
			}
		}

		bulkTransFunc(rows, outChan, badRowChan, stopChan)
	}
}

func NewRowTransformer(name string, transRowFunc TransformRowFunc) TransformFunc {
	return func(inChan <-chan *table.Row, outChan chan<- *table.Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool) {
		for {
			select {
			case <-stopChan:
				return
			default:
			}

			select {
			case row, ok := <-inChan:
				if ok {
					outRowData, badRowDetails := transRowFunc(row)
					outSize := len(outRowData)

					for i := 0; i < outSize; i++ {
						props := row.ClonedMergedProperties(outRowData[i].Properties)
						outRow := table.NewRowWithProperties(outRowData[i].RowData, props)

						outChan <- outRow
					}

					if badRowDetails != "" {
						badRowChan <- &TransformRowFailure{row, name, badRowDetails}
					}
				} else {
					return
				}

			case <-stopChan:
				return
			}
		}
	}
}
