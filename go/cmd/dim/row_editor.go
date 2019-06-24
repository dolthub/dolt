package main

import (
	"context"
	"github.com/gizak/termui/v3"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/nsf/termbox-go"
)

const (
	DarkGreyColorIndex = 240
)

var DarkGrey = termbox.Attribute(DarkGreyColorIndex)

type RowEditor struct {
	dr     *DimRow
	rowY   int
	inY    int
	selCol int
	input  *Input
	colTag uint64
}

func NewRowEditor(dr *DimRow, rowY, inY, selCol int) *RowEditor {
	return &RowEditor{dr, rowY, inY, selCol, nil, schema.InvalidTag}
}

func (re *RowEditor) setSelCol(selCol int) {
	re.selCol = selCol
}

func (re *RowEditor) Resize(rowY, inY int) {
	re.rowY = rowY
	re.inY = inY
}

func (re *RowEditor) Render(tags []uint64, colWidths []int, scrWidth int) {
	x := 0

	rowStrs := strsForRow(tags, re.dr.currentVals)

	for colIdx, cellStr := range rowStrs {
		termbox.SetCell(x, re.rowY, '│', termbox.ColorWhite, termbox.ColorBlack)
		x++

		fg := termbox.ColorBlack
		bg := DarkGrey
		if colIdx == re.selCol {
			fg = termbox.ColorWhite
			bg = termbox.ColorBlue
		}

		cellWidth := colWidths[colIdx]

		if cellWidth == -1 {
			cellWidth = scrWidth - x - 1
		}

		for i := 0; i < cellWidth; i++ {
			ch := ' '
			if i < len(cellStr) {
				ch = rune(cellStr[i])
			}

			termbox.SetCell(x, re.rowY, ch, fg, bg)
			x++
		}
	}

	termbox.Flush()

	if re.input != nil {
		re.input.Render()
	}
}

func (re *RowEditor) editSelected(colTag uint64, colName string, append bool) {
	currValStr := ""
	currVal := re.dr.currentVals[colTag]

	if strVal, ok := currVal.(types.String); ok {
		currValStr = string(strVal)
	}

	re.colTag = colTag
	re.input = NewInput(colName+": ", currValStr, append)
	re.input.Render()
}

func (re *RowEditor) InHandler(ctx context.Context, e termui.Event) (exit, render, releaseFocus bool) {
	if e.ID == "<Escape>" {
		if re.input.initialVal != re.input.Value {
			re.dr.UpdateVal(re.colTag, re.input.Value)
		}

		re.input.Clear()
		re.input = nil
		re.colTag = schema.InvalidTag
		return false, true, true
	} else {
		re.input.KBInputEvent(e)
	}

	return false, false, false
}
