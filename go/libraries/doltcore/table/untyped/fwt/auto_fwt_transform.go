package fwt

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
)

type AutoSizingFWTTransformer struct {
	numSamples int
	widths     map[uint64]int
	rowBuffer  []pipeline.RowWithProps

	sch       schema.Schema
	tooLngBhv TooLongBehavior
	fwtTr     *FWTTransformer
}

func NewAutoSizingFWTTransformer(sch schema.Schema, tooLngBhv TooLongBehavior, numSamples int) *AutoSizingFWTTransformer {
	widths := make(map[uint64]int, sch.GetAllCols().Size())

	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		widths[tag] = 0
		return false
	})

	rowBuffer := make([]pipeline.RowWithProps, 0, 128)

	return &AutoSizingFWTTransformer{numSamples, widths, rowBuffer, sch, tooLngBhv, nil}
}

func (asTr *AutoSizingFWTTransformer) TransformToFWT(inChan <-chan pipeline.RowWithProps, outChan chan<- pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure, stopChan <-chan struct{}) {
RowLoop:
	for {
		select {
		case <-stopChan:
			return
		default:
		}

		select {
		case r, ok := <-inChan:
			if ok {
				asTr.handleRow(r, outChan, badRowChan, stopChan)
			} else {
				break RowLoop
			}
		case <-stopChan:
			return
		}
	}

	asTr.flush(outChan, badRowChan, stopChan)
}

func (asTr *AutoSizingFWTTransformer) handleRow(r pipeline.RowWithProps, outChan chan<- pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure, stopChan <-chan struct{}) {
	if asTr.rowBuffer == nil {
		asTr.processRow(r, outChan, badRowChan)
	} else if asTr.numSamples <= 0 || len(asTr.rowBuffer) < asTr.numSamples {
		r.Row.IterSchema(asTr.sch, func(tag uint64, val types.Value) (stop bool) {
			if !types.IsNull(val) {
				strVal := val.(types.String)
				strLen := len([]rune(string(strVal)))

				width, ok := asTr.widths[tag]
				if ok && strLen > width {
					asTr.widths[tag] = strLen
				}
			}
			return false
		})

		asTr.rowBuffer = append(asTr.rowBuffer, r)
	} else {
		asTr.flush(outChan, badRowChan, stopChan)
	}
}

func (asWr *AutoSizingFWTTransformer) flush(outChan chan<- pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure, stopChan <-chan struct{}) {
	if asWr.fwtTr == nil {
		fwtSch := NewFWTSchemaWithWidths(asWr.sch, asWr.widths)
		asWr.fwtTr = NewFWTTransformer(fwtSch, asWr.tooLngBhv)
	}

	for i := 0; i < len(asWr.rowBuffer); i++ {
		asWr.processRow(asWr.rowBuffer[i], outChan, badRowChan)

		if i%100 == 0 {
			select {
			case <-stopChan:
				return
			default:
			}
		}
	}

	asWr.rowBuffer = nil
}

func (asTr *AutoSizingFWTTransformer) processRow(rowWithProps pipeline.RowWithProps, outChan chan<- pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure) {
	rds, errMsg := asTr.fwtTr.Transform(rowWithProps.Row, rowWithProps.Props)

	if errMsg != "" {
		badRowChan <- &pipeline.TransformRowFailure{
			Row:           rowWithProps.Row,
			TransformName: "Auto Sizing Fixed Width Transform",
			Details:       errMsg,
		}
	} else if len(rds) == 1 {
		propUpdates := rds[0].PropertyUpdates

		outProps := rowWithProps.Props
		if len(propUpdates) > 0 {
			outProps = outProps.Set(propUpdates)
		}

		outRow := pipeline.RowWithProps{rds[0].RowData, outProps}
		outChan <- outRow
	}
}
