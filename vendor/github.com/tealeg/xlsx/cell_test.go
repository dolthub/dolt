package xlsx

import (
	. "gopkg.in/check.v1"
)

type CellSuite struct{}

var _ = Suite(&CellSuite{})

// Test that we can set and get a Value from a Cell
func (s *CellSuite) TestValueSet(c *C) {
	// Note, this test is fairly pointless, it serves mostly to
	// reinforce that this functionality is important, and should
	// the mechanics of this all change at some point, to remind
	// us not to lose this.
	cell := Cell{}
	cell.Value = "A string"
	c.Assert(cell.Value, Equals, "A string")
}

// Test that GetStyle correctly converts the xlsxStyle.Fonts.
func (s *CellSuite) TestGetStyleWithFonts(c *C) {
	font := NewFont(10, "Calibra")
	style := NewStyle()
	style.Font = *font

	cell := &Cell{Value: "123", style: style}
	style = cell.GetStyle()
	c.Assert(style, NotNil)
	c.Assert(style.Font.Size, Equals, 10)
	c.Assert(style.Font.Name, Equals, "Calibra")
}

// Test that SetStyle correctly translates into a xlsxFont element
func (s *CellSuite) TestSetStyleWithFonts(c *C) {
	file := NewFile()
	sheet, _ := file.AddSheet("Test")
	row := sheet.AddRow()
	cell := row.AddCell()
	font := NewFont(12, "Calibra")
	style := NewStyle()
	style.Font = *font
	cell.SetStyle(style)
	style = cell.GetStyle()
	xFont, _, _, _, _ := style.makeXLSXStyleElements()
	c.Assert(xFont.Sz.Val, Equals, "12")
	c.Assert(xFont.Name.Val, Equals, "Calibra")
}

// Test that GetStyle correctly converts the xlsxStyle.Fills.
func (s *CellSuite) TestGetStyleWithFills(c *C) {
	fill := *NewFill("solid", "FF000000", "00FF0000")
	style := NewStyle()
	style.Fill = fill
	cell := &Cell{Value: "123", style: style}
	style = cell.GetStyle()
	_, xFill, _, _, _ := style.makeXLSXStyleElements()
	c.Assert(xFill.PatternFill.PatternType, Equals, "solid")
	c.Assert(xFill.PatternFill.BgColor.RGB, Equals, "00FF0000")
	c.Assert(xFill.PatternFill.FgColor.RGB, Equals, "FF000000")
}

// Test that SetStyle correctly updates xlsxStyle.Fills.
func (s *CellSuite) TestSetStyleWithFills(c *C) {
	file := NewFile()
	sheet, _ := file.AddSheet("Test")
	row := sheet.AddRow()
	cell := row.AddCell()
	fill := NewFill("solid", "00FF0000", "FF000000")
	style := NewStyle()
	style.Fill = *fill
	cell.SetStyle(style)
	style = cell.GetStyle()
	_, xFill, _, _, _ := style.makeXLSXStyleElements()
	xPatternFill := xFill.PatternFill
	c.Assert(xPatternFill.PatternType, Equals, "solid")
	c.Assert(xPatternFill.FgColor.RGB, Equals, "00FF0000")
	c.Assert(xPatternFill.BgColor.RGB, Equals, "FF000000")
}

// Test that GetStyle correctly converts the xlsxStyle.Borders.
func (s *CellSuite) TestGetStyleWithBorders(c *C) {
	border := *NewBorder("thin", "thin", "thin", "thin")
	style := NewStyle()
	style.Border = border
	cell := Cell{Value: "123", style: style}
	style = cell.GetStyle()
	_, _, xBorder, _, _ := style.makeXLSXStyleElements()
	c.Assert(xBorder.Left.Style, Equals, "thin")
	c.Assert(xBorder.Right.Style, Equals, "thin")
	c.Assert(xBorder.Top.Style, Equals, "thin")
	c.Assert(xBorder.Bottom.Style, Equals, "thin")
}

// We can return a string representation of the formatted data
func (l *CellSuite) TestSetFloatWithFormat(c *C) {
	cell := Cell{}
	cell.SetFloatWithFormat(37947.75334343, "yyyy/mm/dd")
	c.Assert(cell.Value, Equals, "37947.75334343")
	c.Assert(cell.NumFmt, Equals, "yyyy/mm/dd")
	c.Assert(cell.Type(), Equals, CellTypeNumeric)
}

func (l *CellSuite) TestSetFloat(c *C) {
	cell := Cell{}
	cell.SetFloat(0)
	c.Assert(cell.Value, Equals, "0")
	cell.SetFloat(0.000005)
	c.Assert(cell.Value, Equals, "5e-06")
	cell.SetFloat(100.0)
	c.Assert(cell.Value, Equals, "100")
	cell.SetFloat(37947.75334343)
	c.Assert(cell.Value, Equals, "37947.75334343")
}

// SafeFormattedValue returns an error for formatting errors
func (l *CellSuite) TestSafeFormattedValueErrorsOnBadFormat(c *C) {
	cell := Cell{Value: "Fudge Cake"}
	cell.NumFmt = "#,##0 ;(#,##0)"
	value, err := cell.SafeFormattedValue()
	c.Assert(value, Equals, "Fudge Cake")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "strconv.ParseFloat: parsing \"Fudge Cake\": invalid syntax")
}

// FormattedValue returns a string containing error text for formatting errors
func (l *CellSuite) TestFormattedValueReturnsErrorAsValueForBadFormat(c *C) {
	cell := Cell{Value: "Fudge Cake"}
	cell.NumFmt = "#,##0 ;(#,##0)"
	value := cell.FormattedValue()
	c.Assert(value, Equals, "strconv.ParseFloat: parsing \"Fudge Cake\": invalid syntax")
}

// We can return a string representation of the formatted data
func (l *CellSuite) TestFormattedValue(c *C) {
	// XXX TODO, this test should probably be split down, and made
	// in terms of SafeFormattedValue, as FormattedValue wraps
	// that function now.
	cell := Cell{Value: "37947.7500001"}
	negativeCell := Cell{Value: "-37947.7500001"}
	smallCell := Cell{Value: "0.007"}
	earlyCell := Cell{Value: "2.1"}

	cell.NumFmt = "general"
	c.Assert(cell.FormattedValue(), Equals, "37947.7500001")
	negativeCell.NumFmt = "general"
	c.Assert(negativeCell.FormattedValue(), Equals, "-37947.7500001")

	// TODO: This test is currently broken.  For a string type cell, I
	// don't think FormattedValue() should be doing a numeric conversion on the value
	// before returning the string.
	cell.NumFmt = "0"
	c.Assert(cell.FormattedValue(), Equals, "37947")

	cell.NumFmt = "#,##0" // For the time being we're not doing
	// this comma formatting, so it'll fall back to the related
	// non-comma form.
	c.Assert(cell.FormattedValue(), Equals, "37947")

	cell.NumFmt = "#,##0.00;(#,##0.00)"
	c.Assert(cell.FormattedValue(), Equals, "37947.75")

	cell.NumFmt = "0.00"
	c.Assert(cell.FormattedValue(), Equals, "37947.75")

	cell.NumFmt = "#,##0.00" // For the time being we're not doing
	// this comma formatting, so it'll fall back to the related
	// non-comma form.
	c.Assert(cell.FormattedValue(), Equals, "37947.75")

	cell.NumFmt = "#,##0 ;(#,##0)"
	c.Assert(cell.FormattedValue(), Equals, "37947")
	negativeCell.NumFmt = "#,##0 ;(#,##0)"
	c.Assert(negativeCell.FormattedValue(), Equals, "(37947)")

	cell.NumFmt = "#,##0 ;[red](#,##0)"
	c.Assert(cell.FormattedValue(), Equals, "37947")
	negativeCell.NumFmt = "#,##0 ;[red](#,##0)"
	c.Assert(negativeCell.FormattedValue(), Equals, "(37947)")

	negativeCell.NumFmt = "#,##0.00;(#,##0.00)"
	c.Assert(negativeCell.FormattedValue(), Equals, "(-37947.75)")

	cell.NumFmt = "0%"
	c.Assert(cell.FormattedValue(), Equals, "3794775%")

	cell.NumFmt = "0.00%"
	c.Assert(cell.FormattedValue(), Equals, "3794775.00%")

	cell.NumFmt = "0.00e+00"
	c.Assert(cell.FormattedValue(), Equals, "3.794775e+04")

	cell.NumFmt = "##0.0e+0" // This is wrong, but we'll use it for now.
	c.Assert(cell.FormattedValue(), Equals, "3.794775e+04")

	cell.NumFmt = "mm-dd-yy"
	c.Assert(cell.FormattedValue(), Equals, "11-22-03")

	cell.NumFmt = "d-mmm-yy"
	c.Assert(cell.FormattedValue(), Equals, "22-Nov-03")
	earlyCell.NumFmt = "d-mmm-yy"
	c.Assert(earlyCell.FormattedValue(), Equals, "1-Jan-00")

	cell.NumFmt = "d-mmm"
	c.Assert(cell.FormattedValue(), Equals, "22-Nov")
	earlyCell.NumFmt = "d-mmm"
	c.Assert(earlyCell.FormattedValue(), Equals, "1-Jan")

	cell.NumFmt = "mmm-yy"
	c.Assert(cell.FormattedValue(), Equals, "Nov-03")

	cell.NumFmt = "h:mm am/pm"
	c.Assert(cell.FormattedValue(), Equals, "6:00 pm")
	smallCell.NumFmt = "h:mm am/pm"
	c.Assert(smallCell.FormattedValue(), Equals, "12:14 am")

	cell.NumFmt = "h:mm:ss am/pm"
	c.Assert(cell.FormattedValue(), Equals, "6:00:00 pm")
	cell.NumFmt = "hh:mm:ss"
	c.Assert(cell.FormattedValue(), Equals, "18:00:00")
	smallCell.NumFmt = "h:mm:ss am/pm"
	c.Assert(smallCell.FormattedValue(), Equals, "12:14:47 am")

	cell.NumFmt = "h:mm"
	c.Assert(cell.FormattedValue(), Equals, "6:00")
	smallCell.NumFmt = "h:mm"
	c.Assert(smallCell.FormattedValue(), Equals, "12:14")
	smallCell.NumFmt = "hh:mm"
	c.Assert(smallCell.FormattedValue(), Equals, "00:14")

	cell.NumFmt = "h:mm:ss"
	c.Assert(cell.FormattedValue(), Equals, "6:00:00")
	cell.NumFmt = "hh:mm:ss"
	c.Assert(cell.FormattedValue(), Equals, "18:00:00")

	smallCell.NumFmt = "hh:mm:ss"
	c.Assert(smallCell.FormattedValue(), Equals, "00:14:47")
	smallCell.NumFmt = "h:mm:ss"
	c.Assert(smallCell.FormattedValue(), Equals, "12:14:47")

	cell.NumFmt = "m/d/yy h:mm"
	c.Assert(cell.FormattedValue(), Equals, "11/22/03 6:00")
	cell.NumFmt = "m/d/yy hh:mm"
	c.Assert(cell.FormattedValue(), Equals, "11/22/03 18:00")
	smallCell.NumFmt = "m/d/yy h:mm"
	c.Assert(smallCell.FormattedValue(), Equals, "12/30/99 12:14") // Note, that's 1899
	smallCell.NumFmt = "m/d/yy hh:mm"
	c.Assert(smallCell.FormattedValue(), Equals, "12/30/99 00:14") // Note, that's 1899
	earlyCell.NumFmt = "m/d/yy hh:mm"
	c.Assert(earlyCell.FormattedValue(), Equals, "1/1/00 02:24") // and 1900
	earlyCell.NumFmt = "m/d/yy h:mm"
	c.Assert(earlyCell.FormattedValue(), Equals, "1/1/00 2:24") // and 1900

	cell.NumFmt = "mm:ss"
	c.Assert(cell.FormattedValue(), Equals, "00:00")
	smallCell.NumFmt = "mm:ss"
	c.Assert(smallCell.FormattedValue(), Equals, "14:47")

	cell.NumFmt = "[hh]:mm:ss"
	c.Assert(cell.FormattedValue(), Equals, "18:00:00")
	cell.NumFmt = "[h]:mm:ss"
	c.Assert(cell.FormattedValue(), Equals, "6:00:00")
	smallCell.NumFmt = "[h]:mm:ss"
	c.Assert(smallCell.FormattedValue(), Equals, "14:47")

	cell.NumFmt = "mmss.0" // I'm not sure about these.
	c.Assert(cell.FormattedValue(), Equals, "0000.0086")
	smallCell.NumFmt = "mmss.0"
	c.Assert(smallCell.FormattedValue(), Equals, "1447.9999")

	cell.NumFmt = "yyyy\\-mm\\-dd"
	c.Assert(cell.FormattedValue(), Equals, "2003\\-11\\-22")

	cell.NumFmt = "dd/mm/yyyy hh:mm:ss"
	c.Assert(cell.FormattedValue(), Equals, "22/11/2003 18:00:00")

	cell.NumFmt = "dd/mm/yy"
	c.Assert(cell.FormattedValue(), Equals, "22/11/03")
	earlyCell.NumFmt = "dd/mm/yy"
	c.Assert(earlyCell.FormattedValue(), Equals, "01/01/00")

	cell.NumFmt = "hh:mm:ss"
	c.Assert(cell.FormattedValue(), Equals, "18:00:00")
	smallCell.NumFmt = "hh:mm:ss"
	c.Assert(smallCell.FormattedValue(), Equals, "00:14:47")

	cell.NumFmt = "dd/mm/yy\\ hh:mm"
	c.Assert(cell.FormattedValue(), Equals, "22/11/03\\ 18:00")

	cell.NumFmt = "yyyy/mm/dd"
	c.Assert(cell.FormattedValue(), Equals, "2003/11/22")

	cell.NumFmt = "yy-mm-dd"
	c.Assert(cell.FormattedValue(), Equals, "03-11-22")

	cell.NumFmt = "d-mmm-yyyy"
	c.Assert(cell.FormattedValue(), Equals, "22-Nov-2003")
	earlyCell.NumFmt = "d-mmm-yyyy"
	c.Assert(earlyCell.FormattedValue(), Equals, "1-Jan-1900")

	cell.NumFmt = "m/d/yy"
	c.Assert(cell.FormattedValue(), Equals, "11/22/03")
	earlyCell.NumFmt = "m/d/yy"
	c.Assert(earlyCell.FormattedValue(), Equals, "1/1/00")

	cell.NumFmt = "m/d/yyyy"
	c.Assert(cell.FormattedValue(), Equals, "11/22/2003")
	earlyCell.NumFmt = "m/d/yyyy"
	c.Assert(earlyCell.FormattedValue(), Equals, "1/1/1900")

	cell.NumFmt = "dd-mmm-yyyy"
	c.Assert(cell.FormattedValue(), Equals, "22-Nov-2003")

	cell.NumFmt = "dd/mm/yyyy"
	c.Assert(cell.FormattedValue(), Equals, "22/11/2003")

	cell.NumFmt = "mm/dd/yy hh:mm am/pm"
	c.Assert(cell.FormattedValue(), Equals, "11/22/03 18:00 pm")
	cell.NumFmt = "mm/dd/yy h:mm am/pm"
	c.Assert(cell.FormattedValue(), Equals, "11/22/03 6:00 pm")

	cell.NumFmt = "mm/dd/yyyy hh:mm:ss"
	c.Assert(cell.FormattedValue(), Equals, "11/22/2003 18:00:00")
	smallCell.NumFmt = "mm/dd/yyyy hh:mm:ss"
	c.Assert(smallCell.FormattedValue(), Equals, "12/30/1899 00:14:47")

	cell.NumFmt = "yyyy-mm-dd hh:mm:ss"
	c.Assert(cell.FormattedValue(), Equals, "2003-11-22 18:00:00")
	smallCell.NumFmt = "yyyy-mm-dd hh:mm:ss"
	c.Assert(smallCell.FormattedValue(), Equals, "1899-12-30 00:14:47")
}

// test setters and getters
func (s *CellSuite) TestSetterGetters(c *C) {
	cell := Cell{}

	cell.SetString("hello world")
	c.Assert(cell.String(), Equals, "hello world")
	c.Assert(cell.Type(), Equals, CellTypeString)

	cell.SetInt(1024)
	intValue, _ := cell.Int()
	c.Assert(intValue, Equals, 1024)
	c.Assert(cell.Type(), Equals, CellTypeNumeric)

	cell.SetInt64(1024)
	int64Value, _ := cell.Int64()
	c.Assert(int64Value, Equals, int64(1024))
	c.Assert(cell.Type(), Equals, CellTypeNumeric)

	cell.SetFloat(1.024)
	float, _ := cell.Float()
	intValue, _ = cell.Int() // convert
	c.Assert(float, Equals, 1.024)
	c.Assert(intValue, Equals, 1)
	c.Assert(cell.Type(), Equals, CellTypeNumeric)

	cell.SetFormula("10+20")
	c.Assert(cell.Formula(), Equals, "10+20")
	c.Assert(cell.Type(), Equals, CellTypeFormula)
}

// TestOddInput is a regression test for #101. When the number format
// was "@" (string), the input below caused a crash in strconv.ParseFloat.
// The solution was to check if cell.Value was both a CellTypeString and
// had a NumFmt of "general" or "@" and short-circuit FormattedValue() if so.
func (s *CellSuite) TestOddInput(c *C) {
	cell := Cell{}
	odd := `[1],[12,"DATE NOT NULL DEFAULT '0000-00-00'"]`
	cell.Value = odd
	cell.NumFmt = "@"
	c.Assert(cell.String(), Equals, odd)
}

// TestBool tests basic Bool getting and setting booleans.
func (s *CellSuite) TestBool(c *C) {
	cell := Cell{}
	cell.SetBool(true)
	c.Assert(cell.Value, Equals, "1")
	c.Assert(cell.Bool(), Equals, true)
	cell.SetBool(false)
	c.Assert(cell.Value, Equals, "0")
	c.Assert(cell.Bool(), Equals, false)
}

// TestStringBool tests calling Bool on a non CellTypeBool value.
func (s *CellSuite) TestStringBool(c *C) {
	cell := Cell{}
	cell.SetInt(0)
	c.Assert(cell.Bool(), Equals, false)
	cell.SetInt(1)
	c.Assert(cell.Bool(), Equals, true)
	cell.SetString("")
	c.Assert(cell.Bool(), Equals, false)
	cell.SetString("0")
	c.Assert(cell.Bool(), Equals, true)
}
