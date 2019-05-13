package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
)

// NamedTransform is a struct containing a TransformFunc and the name of the transform being applied.  If an error occurs
// during processing this name will be provided as the TransformName in any TransformRowFailure error.
type NamedTransform struct {
	// The name of the transform (If an error occurs during processing this name will be provided as the TransformName
	// in any TransformRowFailure error.
	Name string

	// Func is the TransformFunc being applied
	Func TransformFunc
}

// NewNamedTransform returns a NamedTransform object from a name and a TransformRowFunc.  The returned NamedTransform
// will have its Func member set to be a TransformFunc that handles input, output, and stop channel processing, along
// with error handling and it will call the given TransformRowFunc for every row.
func NewNamedTransform(name string, transRowFunc TransformRowFunc) NamedTransform {
	transformer := newRowTransformer(name, transRowFunc)
	return NamedTransform{name, transformer}
}

// TransformedRowResult is what will be returned from each stage of a transform
type TransformedRowResult struct {
	// RowData is the new row that should be passed on to the next stage
	RowData row.Row

	// PropertyUpdates are mutations that should be applied to the row's properties
	PropertyUpdates map[string]interface{}
}

// TransformFunc reads rows from the inChan, transforms them, and then writes them to the outChan.  If an error occurs
// processing a row a TransformRowFailure will be written to the failure channel, and if the stopChan is closed it should
// exit all processing.
type TransformFunc func(inChan <-chan RowWithProps, outChan chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, stopChan <-chan struct{})

// TransformRowFunc processes a single row and it's properties and can return 0 or more TransformRowResults per row. If
// the row being processed is bad it should return nil, and a string containing details of the row problem.
type TransformRowFunc func(inRow row.Row, props ReadableMap) (rowData []*TransformedRowResult, badRowDetails string)

func newRowTransformer(name string, transRowFunc TransformRowFunc) TransformFunc {
	return func(inChan <-chan RowWithProps, outChan chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, stopChan <-chan struct{}) {
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
