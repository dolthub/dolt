package main

import (
	"context"
	"fmt"
	"log"

	"github.com/gizak/termui/v3"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func uiWinHeightToRowCount(height int) int {
	rc := (height - 5) / 2

	if rc < 0 {
		return 0
	}

	return rc
}

func rowToUiPos(n int) int {
	return (n * 2) + 3
}

type KBInput func(context.Context, termui.Event) (exit, render, releaseFocus bool)

func getHeaders(sch schema.Schema) ([]string, []uint64) {
	pkCols := sch.GetPKCols()
	nonPKCols := sch.GetNonPKCols()

	i := 0
	headers := make([]string, pkCols.Size()+nonPKCols.Size())
	tagOrder := make([]uint64, pkCols.Size()+nonPKCols.Size())
	pkCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		headers[i] = col.Name
		tagOrder[i] = tag
		i++
		return false
	})

	nonPKCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		headers[i] = col.Name
		tagOrder[i] = tag
		i++
		return false
	})

	return headers, tagOrder
}

type Dim struct {
	selRow  int
	selCol  int
	dw      *DataWindow
	tr      *TableRenderer
	re      *RowEditor
	inFocus KBInput
}

func New(ctx context.Context, sch schema.Schema, rowData types.Map) *Dim {
	toUntypedMapping := rowconv.TypedToUntypedMapping(sch)
	toUntyped, err := rowconv.NewRowConverter(toUntypedMapping)

	if err != nil {
		panic(err)
	}

	toTypedMapping := rowconv.InvertMapping(toUntypedMapping)
	toTyped, err := rowconv.NewRowConverter(toTypedMapping)

	if err != nil {
		panic(err)
	}

	dw := NewDataWindow(ctx, 1, rowData, toUntyped, toTyped)
	tr := NewRenderer(dw, sch)
	re := NewRowEditor(tr.dw.dimRows[0], 0, 0, 0)

	dim := &Dim{0, 0, dw, tr, re, nil}
	dim.inFocus = dim.AppInput

	return dim
}

func (dim *Dim) Run(ctx context.Context) types.Map {
	if err := termui.Init(); err != nil {
		log.Fatalf("failed to initialize termui: %v\n", err)
	}

	defer termui.Close()

	termui.Theme.Table.Text = termui.NewStyle(termui.ColorBlack, termui.ColorBlack)

	dim.eventLoop(ctx)

	return dim.dw.data
}

func (dim *Dim) eventLoop(ctx context.Context) {
	width, height := termui.TerminalDimensions()
	dim.dw.Resize(ctx, uiWinHeightToRowCount(height), 0)
	dim.re.Resize(rowToUiPos(0), height-1)

	shouldRender := true
	exit := false

	uiEvents := termui.PollEvents()
	for {
		if shouldRender {
			termui.Clear()
			dim.tr.render(width)
			dim.re.Render(dim.tr.tags, dim.tr.colWidths, width)
			shouldRender = false
		}

		if exit {
			break
		}

		e := <-uiEvents
		log.Println(e)
		if e.Type == termui.ResizeEvent {
			newSize := e.Payload.(termui.Resize)

			width = newSize.Width
			height = newSize.Height

			dim.selRow = dim.dw.Resize(ctx, uiWinHeightToRowCount(height), dim.selRow)
			dim.re.Resize(rowToUiPos(dim.selRow), height-1)

			shouldRender = true
		} else if e.Type == termui.MouseEvent {

		} else if e.Type == termui.KeyboardEvent {
			var releaseFocus bool
			exit, shouldRender, releaseFocus = dim.inFocus(ctx, e)

			if releaseFocus {
				dim.inFocus = dim.AppInput
			}
		}
	}
}

func (dim *Dim) AppInput(ctx context.Context, e termui.Event) (exit, render, releaseFocus bool) {
	switch e.ID {
	case "k", "<Up>":
		dim.PrevRow()
		return false, true, false
	case "j", "<Down>":
		dim.NextRow(ctx)
		return false, true, false
	case "l", "<Right>":
		if dim.selCol+1 < len(dim.tr.headers) {
			dim.selCol++
			dim.re.setSelCol(dim.selCol)
		}
		return false, true, false

	case "h", "<Left>":
		if dim.selCol-1 >= 0 {
			dim.selCol--
			dim.re.setSelCol(dim.selCol)
		}
		return false, true, false

	case "a":
		dim.re.editSelected(dim.tr.tags[dim.selCol], dim.tr.headers[dim.selCol], true)
		dim.inFocus = dim.re.InHandler
		return false, false, false

	case "i":
		dim.re.editSelected(dim.tr.tags[dim.selCol], dim.tr.headers[dim.selCol], false)
		dim.inFocus = dim.re.InHandler
		return false, false, false

	case ":":
		dim.updateDWFromRE()
		colonPrompt := NewColonPrompt(dim)
		dim.inFocus = colonPrompt.InHandler
	}

	return false, false, false
}

func (dim *Dim) NextRow(ctx context.Context) {
	dim.updateDWFromRE()
	if dim.dw.CanMoveDown() && dim.selRow == dim.dw.Size()/2 {
		dim.dw.MoveDown(ctx)
		dim.changeSelectedRow()
	} else if dim.selRow < dim.dw.Size()-1 && dim.selRow < len(dim.dw.dimRows)-1 {
		dim.selRow++
		dim.changeSelectedRow()
	}
}

func (dim *Dim) PrevRow() {
	dim.updateDWFromRE()
	if dim.dw.CanMoveUp() && dim.selRow == dim.dw.Size()/2 {
		dim.dw.MoveUp()
		dim.changeSelectedRow()
	} else if !dim.dw.CanMoveUp() && dim.selRow > 0 || (dim.selRow > dim.dw.Size()/2) {
		dim.selRow--
		dim.changeSelectedRow()
	}
}

func (dim *Dim) updateDWFromRE() {
	if dim.re.dr.HasChanged() {
		dim.dw.UpdateRow(dim.selRow)
	}
}

func (dim *Dim) changeSelectedRow() {
	dr := dim.dw.NthVisibleRow(dim.selRow)

	if dr == nil {
		panic(fmt.Sprintf("panic - selRow: %d. dataSize: %d", dim.selRow, len(dim.dw.dimRows)))
	}

	_, height := termui.TerminalDimensions()
	dim.re = NewRowEditor(dr, rowToUiPos(dim.selRow), height-1, dim.selCol)
}

func (dim *Dim) FlushEdits(ctx context.Context) {
	dim.dw.FlushEdits(ctx)
}

func (dim *Dim) HasEdits() bool {
	return dim.dw.HasEdits()
}
