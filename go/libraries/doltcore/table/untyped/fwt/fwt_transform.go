package fwt

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
)

type TooLongBehavior int

const (
	ErrorWhenTooLong TooLongBehavior = iota
	SkipRowWhenTooLong
	TruncateWhenTooLong
	HashFillWhenTooLong
	PrintAllWhenTooLong
)

type FWTTransformer struct {
	fwtSch    *FWTSchema
	colBuffs  map[uint64][]byte
	tooLngBhv TooLongBehavior
}

func NewFWTTransformer(fwtSch *FWTSchema, tooLngBhv TooLongBehavior) *FWTTransformer {
	numFields := fwtSch.Sch.GetAllCols().Size()
	colBuffs := make(map[uint64][]byte, numFields)

	for tag, width := range fwtSch.TagToWidth {
		colBuffs[tag] = make([]byte, width)
	}

	return &FWTTransformer{fwtSch, colBuffs, tooLngBhv}
}

func (fwtTr *FWTTransformer) Transform(r row.Row, props pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {
	sch := fwtTr.fwtSch.Sch
	destFields := make(row.TaggedValues)

	for tag, colWidth := range fwtTr.fwtSch.TagToWidth {
		buf := fwtTr.colBuffs[tag]

		if colWidth != 0 {
			var str string
			val, _ := r.GetColVal(tag)

			if !types.IsNull(val) {
				str = string(val.(types.String))
			}

			if len(str) > colWidth {
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

			if len(str) > colWidth {
				buf = []byte(str)
			} else {
				n := copy(buf, str)

				for ; n < colWidth; n++ {
					buf[n] = ' '
				}
			}

		}

		destFields[tag] = types.String(buf)
	}

	r = row.New(sch, destFields)
	return []*pipeline.TransformedRowResult{{RowData: r}}, ""
}

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

	sch.GetAllCols().ItrUnsorted(func(tag uint64, col schema.Column) (stop bool) {
		widths[tag] = 0
		return false
	})

	rowBuffer := make([]pipeline.RowWithProps, 0, 128)

	return &AutoSizingFWTTransformer{numSamples, widths, rowBuffer, sch, tooLngBhv, nil}
}

func (asTr *AutoSizingFWTTransformer) TransformToFWT(inChan <-chan pipeline.RowWithProps, outChan chan<- pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure, stopChan <-chan bool) {
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

func (asTr *AutoSizingFWTTransformer) handleRow(r pipeline.RowWithProps, outChan chan<- pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure, stopChan <-chan bool) {
	if asTr.rowBuffer == nil {
		asTr.processRow(r, outChan, badRowChan)
	} else if asTr.numSamples <= 0 || len(asTr.rowBuffer) < asTr.numSamples {
		r.Row.IterCols(func(tag uint64, val types.Value) (stop bool) {
			if !types.IsNull(val) {
				strVal := val.(types.String)
				strLen := len(string(strVal))

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

func (asWr *AutoSizingFWTTransformer) flush(outChan chan<- pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure, stopChan <-chan bool) {
	if asWr.fwtTr == nil {
		fwtSch := NewFWTSchemaWithWidths(asWr.sch, asWr.widths)
		asWr.fwtTr = NewFWTTransformer(fwtSch, asWr.tooLngBhv)
	}

	for i := 0; i < len(asWr.rowBuffer); i++ {
		asWr.processRow(asWr.rowBuffer[i], outChan, badRowChan)

		if i%10 == 0 {
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
