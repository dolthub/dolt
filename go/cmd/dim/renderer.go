package main

import (
	"github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
)

type TableRenderer struct {
	headers   []string
	tags      []uint64
	dw        *DataWindow
	colWidths []int
}

func NewRenderer(dw *DataWindow, sch schema.Schema) *TableRenderer {
	headers, orderedTags := getHeaders(sch)

	return &TableRenderer{
		headers,
		orderedTags,
		dw,
		make([]int, len(headers)),
	}
}

/*func (tr *TableRenderer) KBInput(e termui.Event) (done bool) {
	if tr.input != nil {
		inDone := tr.input.KBInput(e)

		if inDone {
			tr.input = nil
		}

		return
	}

	switch e.ID {
	case "q", "<C-c>":
		return true
	case "k", "<Up>":
		tr.SelPrevRow()
		tr.render()
	case "j", "<Down>":
		tr.SelNextRow()
		tr.render()
	case "l", "<Right>":
		tr.SelNextCol()
		tr.render()
	case "h", "<Left>":
		tr.SelPrevCol()
		tr.render()
	case "a":
		tr.editSelected(true)
	case "i":
		tr.editSelected(false)
	}

	return false
}*/

func (tr *TableRenderer) render(width int) {
	numRows := tr.dw.Size()
	cells := [][]string{tr.headers}
	tr.dw.IterWindow(func(dimRow *DimRow) {
		cells = append(cells, tr.strsForRow(dimRow.currentVals))
	})

	tr.colWidths = tr.getColWidths(cells)

	uiTable := widgets.NewTable()
	uiTable.TextStyle = termui.NewStyle(termui.ColorWhite, termui.ColorBlack)
	uiTable.SetRect(0, 0, width, 2*numRows+3)
	uiTable.ColumnWidths = tr.colWidths
	uiTable.Rows = cells

	termui.Render(uiTable)
}

func (tr *TableRenderer) strsForRow(vals row.TaggedValues) []string {
	return strsForRow(tr.tags, vals)
}

func strsForRow(tags []uint64, vals row.TaggedValues) []string {
	colVals := make([]string, len(tags))
	for i, tag := range tags {
		val, _ := vals[tag]

		if !types.IsNull(val) {
			colVals[i] = string(val.(types.String))
		} else {
			colVals[i] = ""
		}
	}

	return colVals
}

func (tr *TableRenderer) getColWidths(cells [][]string) []int {
	colWidths := make([]int, len(tr.headers))

	for i, headerStr := range tr.headers {
		colWidths[i] = len(headerStr)
	}

	for _, rowCells := range cells {
		for i, cellStr := range rowCells {
			strWidth := len(cellStr)

			if strWidth > colWidths[i] {
				colWidths[i] = strWidth
			}
		}
	}

	colWidths[len(tr.headers)-1] = -1

	return colWidths
}
