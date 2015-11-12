package xlsx

import (
	"bytes"
	"encoding/xml"

	. "gopkg.in/check.v1"
)

type WorksheetSuite struct{}

var _ = Suite(&WorksheetSuite{})

// Test we can succesfully unmarshal the sheetN.xml files within and
// XLSX file into an xlsxWorksheet struct (and it's related children).
func (w *WorksheetSuite) TestUnmarshallWorksheet(c *C) {
	var sheetxml = bytes.NewBufferString(
		`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
                   xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
          <sheetPr filterMode="false">
            <pageSetUpPr fitToPage="false"/>
          </sheetPr>
          <dimension ref="A1:B2"/>
          <sheetViews>
            <sheetView colorId="64"
                       defaultGridColor="true"
                       rightToLeft="false"
                       showFormulas="false"
                       showGridLines="true"
                       showOutlineSymbols="true"
                       showRowColHeaders="true"
                       showZeros="true"
                       tabSelected="true"
                       topLeftCell="A1"
                       view="normal"
                       windowProtection="false"
                       workbookViewId="0"
                       zoomScale="100"
                       zoomScaleNormal="100"
                       zoomScalePageLayoutView="100">
              <selection activeCell="B2"
                         activeCellId="0"
                         pane="topLeft"
                         sqref="B2"/>
            </sheetView>
          </sheetViews>
          <sheetFormatPr defaultRowHeight="15" defaultColWidth="8">
          </sheetFormatPr>
          <cols>
            <col collapsed="false"
                 hidden="false"
                 max="1025"
                 min="1"
                 style="0"
                 width="10.5748987854251"/>
          </cols>
          <sheetData>
            <row collapsed="false"
                 customFormat="false"
                 customHeight="false"
                 hidden="false"
                 ht="14.9"
                 outlineLevel="0"
                 r="1">
              <c r="A1"
                 s="1"
                 t="s">
                <v>0</v>
              </c>
              <c r="B1"
                 s="0"
                 t="s">
                <v>1</v>
              </c>
            </row>
            <row collapsed="false"
                 customFormat="false"
                 customHeight="false"
                 hidden="false"
                 ht="14.9"
                 outlineLevel="0"
                 r="2">
              <c r="A2"
                 s="0"
                 t="s">
                <v>2</v>
              </c>
              <c r="B2"
                 s="2"
                 t="s">
                <v>3</v>
              </c>
            </row>
          </sheetData>
          <printOptions headings="false"
                        gridLines="false"
                        gridLinesSet="true"
                        horizontalCentered="false"
                        verticalCentered="false"/>
          <pageMargins left="0.7"
                       right="0.7"
                       top="0.7875"
                       bottom="0.7875"
                       header="0.511805555555555"
                       footer="0.511805555555555"/>
          <pageSetup blackAndWhite="false"
                     cellComments="none"
                     copies="1"
                     draft="false"
                     firstPageNumber="0"
                     fitToHeight="1"
                     fitToWidth="1"
                     horizontalDpi="300"
                     orientation="portrait"
                     pageOrder="downThenOver"
                     paperSize="9"
                     scale="100"
                     useFirstPageNumber="false"
                     usePrinterDefaults="false"
                     verticalDpi="300"/>
          <headerFooter differentFirst="false"
                        differentOddEven="false">
            <oddHeader>
            </oddHeader>
            <oddFooter>
            </oddFooter>
          </headerFooter>
        </worksheet>`)
	worksheet := new(xlsxWorksheet)
	err := xml.NewDecoder(sheetxml).Decode(worksheet)
	c.Assert(err, IsNil)
	c.Assert(worksheet.Dimension.Ref, Equals, "A1:B2")
	c.Assert(worksheet.SheetData.Row, HasLen, 2)
	c.Assert(worksheet.SheetFormatPr.DefaultRowHeight, Equals, 15.0)
	c.Assert(worksheet.SheetFormatPr.DefaultColWidth, Equals, 8.0)
	row := worksheet.SheetData.Row[0]
	c.Assert(row.R, Equals, 1)
	c.Assert(row.C, HasLen, 2)
	cell := row.C[0]
	c.Assert(cell.R, Equals, "A1")
	c.Assert(cell.T, Equals, "s")
	c.Assert(cell.V, Equals, "0")
}

// MergeCells information is correctly read from the worksheet.
func (w *WorksheetSuite) TestUnmarshallWorksheetWithMergeCells(c *C) {
	var sheetxml = bytes.NewBufferString(
		`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:mx="http://schemas.microsoft.com/office/mac/excel/2008/main" xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006" xmlns:mv="urn:schemas-microsoft-com:mac:vml" xmlns:x14="http://schemas.microsoft.com/office/spreadsheetml/2009/9/main" xmlns:x14ac="http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac" xmlns:xm="http://schemas.microsoft.com/office/excel/2006/main">
  <sheetViews>
    <sheetView workbookViewId="0"/>
  </sheetViews>
  <sheetFormatPr customHeight="1" defaultColWidth="17.29" defaultRowHeight="15.0"/>
  <cols>
    <col customWidth="1" min="1" max="6" width="14.43"/>
  </cols>
  <sheetData>
    <row r="1" ht="15.75" customHeight="1">
      <c r="A1" s="1" t="s">
        <v>0</v>
      </c>
    </row>
    <row r="2" ht="15.75" customHeight="1">
      <c r="A2" s="1" t="s">
        <v>1</v>
      </c>
      <c r="B2" s="1" t="s">
        <v>2</v>
      </c>
    </row>
  </sheetData>
  <mergeCells count="1">
    <mergeCell ref="A1:B1"/>
  </mergeCells>
  <drawing r:id="rId1"/>
</worksheet>
`)
	worksheet := new(xlsxWorksheet)
	err := xml.NewDecoder(sheetxml).Decode(worksheet)
	c.Assert(err, IsNil)
	c.Assert(worksheet.MergeCells, NotNil)
	c.Assert(worksheet.MergeCells.Count, Equals, 1)
	mergeCell := worksheet.MergeCells.Cells[0]
	c.Assert(mergeCell.Ref, Equals, "A1:B1")
}

// MergeCells.getExtents returns the horizontal and vertical extent of
// a merge that begins at a given reference.
func (w *WorksheetSuite) TestMergeCellsGetExtent(c *C) {
	mc := xlsxMergeCells{Count: 1}
	mc.Cells = make([]xlsxMergeCell, 1)
	mc.Cells[0] = xlsxMergeCell{Ref: "A1:B5"}
	h, v, err := mc.getExtent("A1")
	c.Assert(err, IsNil)
	c.Assert(h, Equals, 1)
	c.Assert(v, Equals, 4)
}
