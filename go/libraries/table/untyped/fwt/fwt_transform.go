package fwt

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
)

type TooLongBehavior int

const (
	ErrorWhenTooLong TooLongBehavior = iota
	SkipRowWhenTooLong
	ConCatWhenTooLong
	HashFillWhenTooLong
)

type FWTTransformer struct {
	fwtSch    *FWTSchema
	colBuffs  [][]byte
	tooLngBhv TooLongBehavior
}

func NewFWTTransformer(fwtSch *FWTSchema, tooLngBhv TooLongBehavior) *FWTTransformer {
	numFields := fwtSch.Sch.NumFields()
	colBuffs := make([][]byte, numFields)

	for i := 0; i < fwtSch.Sch.NumFields(); i++ {
		colBuffs[i] = make([]byte, fwtSch.Widths[i])
	}

	return &FWTTransformer{fwtSch, colBuffs, tooLngBhv}
}

func (fwtTr *FWTTransformer) Transform(row *table.Row) ([]*table.TransformedRowResult, string) {
	sch := row.GetSchema()
	rowData := row.CurrData()
	destFields := make([]types.Value, sch.NumFields())
	for i := 0; i < sch.NumFields(); i++ {
		colWidth := fwtTr.fwtSch.Widths[i]
		buf := fwtTr.colBuffs[i]

		if colWidth != 0 {
			var str string
			val, fld := rowData.GetField(i)
			if !types.IsNull(val) {
				str = string(val.(types.String))
			}

			if len(str) > colWidth {
				switch fwtTr.tooLngBhv {
				case ErrorWhenTooLong:
					return nil, "Value for " + fld.NameStr() + " too long."
				case SkipRowWhenTooLong:
					return nil, ""
				case ConCatWhenTooLong:
					str = str[0:colWidth]
				case HashFillWhenTooLong:
					str = fwtTr.fwtSch.NoFitStrs[i]
				}
			}

			n := copy(buf, str)
			for ; n < colWidth; n++ {
				buf[n] = ' '
			}
		}

		destFields[i] = types.String(buf)
	}

	rd := table.RowDataFromValues(fwtTr.fwtSch.Sch, destFields)
	return []*table.TransformedRowResult{{RowData: rd}}, ""
}

type AutoSizingFWTTransformer struct {
	numSamples int
	widths     []int
	rowBuffer  []*table.Row

	sch       *schema.Schema
	tooLngBhv TooLongBehavior
	fwtTr     *FWTTransformer
}

func NewAutoSizingFWTTransformer(sch *schema.Schema, tooLngBhv TooLongBehavior, numSamples int) *AutoSizingFWTTransformer {
	widths := make([]int, sch.NumFields())
	rowBuffer := make([]*table.Row, 0, 128)

	return &AutoSizingFWTTransformer{numSamples, widths, rowBuffer, sch, tooLngBhv, nil}
}

func (asTr *AutoSizingFWTTransformer) TransformToFWT(inChan <-chan *table.Row, outChan chan<- *table.Row, badRowChan chan<- *table.TransformRowFailure, stopChan <-chan bool) {
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
				asTr.handleRow(row, outChan, badRowChan, stopChan)
			} else {
				break RowLoop
			}
		case <-stopChan:
			return
		}
	}

	asTr.flush(outChan, badRowChan, stopChan)
}

func (asTr *AutoSizingFWTTransformer) handleRow(row *table.Row, outChan chan<- *table.Row, badRowChan chan<- *table.TransformRowFailure, stopChan <-chan bool) {
	if asTr.rowBuffer == nil {
		asTr.processRow(row, outChan, badRowChan)
	} else if asTr.numSamples <= 0 || len(asTr.rowBuffer) < asTr.numSamples {
		sch := row.GetSchema()
		rowData := row.CurrData()
		for i := 0; i < sch.NumFields(); i++ {
			val, _ := rowData.GetField(i)

			if !types.IsNull(val) {
				strVal := val.(types.String)
				strLen := len(string(strVal))

				if strLen > asTr.widths[i] {
					asTr.widths[i] = strLen
				}
			}
		}

		asTr.rowBuffer = append(asTr.rowBuffer, row)
	} else {
		asTr.flush(outChan, badRowChan, stopChan)
	}
}

func (asWr *AutoSizingFWTTransformer) flush(outChan chan<- *table.Row, badRowChan chan<- *table.TransformRowFailure, stopChan <-chan bool) {
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

func (asTr *AutoSizingFWTTransformer) processRow(row *table.Row, outChan chan<- *table.Row, badRowChan chan<- *table.TransformRowFailure) {
	rds, errMsg := asTr.fwtTr.Transform(row)

	if errMsg != "" {
		badRowChan <- &table.TransformRowFailure{
			BadRow:        row,
			TransformName: "Auto Sizing Fixed Width Transform",
			BadRowDetails: errMsg,
		}
	} else if len(rds) == 1 {
		props := row.ClonedMergedProperties(rds[0].Properties)
		outRow := table.NewRowWithProperties(rds[0].RowData, props)
		outChan <- outRow
	}
}
