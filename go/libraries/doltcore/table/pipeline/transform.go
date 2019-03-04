package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
)

type TransformedRowResult struct {
	RowData         row.Row
	PropertyUpdates map[string]interface{}
}

type TransFailCallback func(row RowWithProps, errDetails string)
type TransformFunc func(inChan <-chan RowWithProps, outChan chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool)
type TransformRowFunc func(inRow row.Row, props ReadableMap) (rowData []*TransformedRowResult, badRowDetails string)

func NewRowTransformer(name string, transRowFunc TransformRowFunc) TransformFunc {
	return func(inChan <-chan RowWithProps, outChan chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool) {
		for {
			select {
			case <-stopChan:
				return
			default:
			}

			select {
			case r, ok := <-inChan:
				if ok {
					outRowData, badRowDetails := transRowFunc(r.Row, r.Props)
					outSize := len(outRowData)

					for i := 0; i < outSize; i++ {
						propUpdates := outRowData[i].PropertyUpdates

						outProps := r.Props
						if len(propUpdates) > 0 {
							outProps = outProps.Set(propUpdates)
						}

						outRow := RowWithProps{outRowData[i].RowData, outProps}
						outChan <- outRow
					}

					if badRowDetails != "" {
						badRowChan <- &TransformRowFailure{r.Row, name, badRowDetails}
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
