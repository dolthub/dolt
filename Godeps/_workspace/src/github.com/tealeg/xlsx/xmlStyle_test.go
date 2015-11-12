package xlsx

import (
	. "gopkg.in/check.v1"
)

type XMLStyleSuite struct{}

var _ = Suite(&XMLStyleSuite{})

// Test we produce valid output for an empty style file.
func (x *XMLStyleSuite) TestMarshalEmptyXlsxStyleSheet(c *C) {
	styles := newXlsxStyleSheet(nil)
	result, err := styles.Marshal()
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, `<?xml version="1.0" encoding="UTF-8"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"></styleSheet>`)
}

// Test we produce valid output for a style file with one font definition.
func (x *XMLStyleSuite) TestMarshalXlsxStyleSheetWithAFont(c *C) {
	styles := newXlsxStyleSheet(nil)
	styles.Fonts = xlsxFonts{}
	styles.Fonts.Count = 1
	styles.Fonts.Font = make([]xlsxFont, 1)
	font := xlsxFont{}
	font.Sz.Val = "10"
	font.Name.Val = "Andale Mono"
	font.B = &xlsxVal{}
	font.I = &xlsxVal{}
	font.U = &xlsxVal{}
	styles.Fonts.Font[0] = font

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font><sz val="10"/><name val="Andale Mono"/><b/><i/><u/></font></fonts></styleSheet>`
	result, err := styles.Marshal()
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, expected)
}

// Test we produce valid output for a style file with one fill definition.
func (x *XMLStyleSuite) TestMarshalXlsxStyleSheetWithAFill(c *C) {
	styles := newXlsxStyleSheet(nil)
	styles.Fills = xlsxFills{}
	styles.Fills.Count = 1
	styles.Fills.Fill = make([]xlsxFill, 1)
	fill := xlsxFill{}
	patternFill := xlsxPatternFill{
		PatternType: "solid",
		FgColor:     xlsxColor{RGB: "#FFFFFF"},
		BgColor:     xlsxColor{RGB: "#000000"}}
	fill.PatternFill = patternFill
	styles.Fills.Fill[0] = fill

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fills count="1"><fill><patternFill patternType="solid"><fgColor rgb="#FFFFFF"/><bgColor rgb="#000000"/></patternFill></fill></fills></styleSheet>`
	result, err := styles.Marshal()
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, expected)
}

// Test we produce valid output for a style file with one border definition.
func (x *XMLStyleSuite) TestMarshalXlsxStyleSheetWithABorder(c *C) {
	styles := newXlsxStyleSheet(nil)
	styles.Borders = xlsxBorders{}
	styles.Borders.Count = 1
	styles.Borders.Border = make([]xlsxBorder, 1)
	border := xlsxBorder{}
	border.Left.Style = "solid"
	border.Top.Style = "none"
	styles.Borders.Border[0] = border

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><borders count="1"><border><left style="solid"></left><top style="none"></top></border></borders></styleSheet>`
	result, err := styles.Marshal()
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, expected)
}

// Test we produce valid output for a style file with one cellStyleXf definition.
func (x *XMLStyleSuite) TestMarshalXlsxStyleSheetWithACellStyleXf(c *C) {
	styles := newXlsxStyleSheet(nil)
	styles.CellStyleXfs = xlsxCellStyleXfs{}
	styles.CellStyleXfs.Count = 1
	styles.CellStyleXfs.Xf = make([]xlsxXf, 1)
	xf := xlsxXf{}
	xf.ApplyAlignment = true
	xf.ApplyBorder = true
	xf.ApplyFont = true
	xf.ApplyFill = true
	xf.ApplyProtection = true
	xf.BorderId = 0
	xf.FillId = 0
	xf.FontId = 0
	xf.NumFmtId = 0
	xf.Alignment = xlsxAlignment{
		Horizontal:   "left",
		Indent:       1,
		ShrinkToFit:  true,
		TextRotation: 0,
		Vertical:     "middle",
		WrapText:     false}
	styles.CellStyleXfs.Xf[0] = xf

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cellStyleXfs count="1"><xf applyAlignment="1" applyBorder="1" applyFont="1" applyFill="1" applyNumberFormat="0" applyProtection="1" borderId="0" fillId="0" fontId="0" numFmtId="0"><alignment horizontal="left" indent="1" shrinkToFit="1" textRotation="0" vertical="middle" wrapText="0"/></xf></cellStyleXfs></styleSheet>`
	result, err := styles.Marshal()
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, expected)
}

// Test we produce valid output for a style file with one cellXf
// definition.
func (x *XMLStyleSuite) TestMarshalXlsxStyleSheetWithACellXf(c *C) {
	styles := newXlsxStyleSheet(nil)
	styles.CellXfs = xlsxCellXfs{}
	styles.CellXfs.Count = 1
	styles.CellXfs.Xf = make([]xlsxXf, 1)
	xf := xlsxXf{}
	xf.ApplyAlignment = true
	xf.ApplyBorder = true
	xf.ApplyFont = true
	xf.ApplyFill = true
	xf.ApplyNumberFormat = true
	xf.ApplyProtection = true
	xf.BorderId = 0
	xf.FillId = 0
	xf.FontId = 0
	xf.NumFmtId = 0
	xf.Alignment = xlsxAlignment{
		Horizontal:   "left",
		Indent:       1,
		ShrinkToFit:  true,
		TextRotation: 0,
		Vertical:     "middle",
		WrapText:     false}
	styles.CellXfs.Xf[0] = xf

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cellXfs count="1"><xf applyAlignment="1" applyBorder="1" applyFont="1" applyFill="1" applyNumberFormat="1" applyProtection="1" borderId="0" fillId="0" fontId="0" numFmtId="0"><alignment horizontal="left" indent="1" shrinkToFit="1" textRotation="0" vertical="middle" wrapText="0"/></xf></cellXfs></styleSheet>`
	result, err := styles.Marshal()
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, expected)
}

// Test we produce valid output for a style file with one NumFmt
// definition.
func (x *XMLStyleSuite) TestMarshalXlsxStyleSheetWithANumFmt(c *C) {
	styles := &xlsxStyleSheet{}
	styles.NumFmts = xlsxNumFmts{}
	styles.NumFmts.NumFmt = make([]xlsxNumFmt, 0)
	numFmt := xlsxNumFmt{NumFmtId: 164, FormatCode: "GENERAL"}
	styles.addNumFmt(numFmt)

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><numFmts count="1"><numFmt numFmtId="164" formatCode="GENERAL"/></numFmts></styleSheet>`
	result, err := styles.Marshal()
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, expected)
}

func (x *XMLStyleSuite) TestFontEquals(c *C) {
	fontA := xlsxFont{Sz: xlsxVal{Val: "11"},
		Color:  xlsxColor{RGB: "FFFF0000"},
		Name:   xlsxVal{Val: "Calibri"},
		Family: xlsxVal{Val: "2"},
		B:      &xlsxVal{},
		I:      &xlsxVal{},
		U:      &xlsxVal{}}
	fontB := xlsxFont{Sz: xlsxVal{Val: "11"},
		Color:  xlsxColor{RGB: "FFFF0000"},
		Name:   xlsxVal{Val: "Calibri"},
		Family: xlsxVal{Val: "2"},
		B:      &xlsxVal{},
		I:      &xlsxVal{},
		U:      &xlsxVal{}}

	c.Assert(fontA.Equals(fontB), Equals, true)
	fontB.Sz.Val = "12"
	c.Assert(fontA.Equals(fontB), Equals, false)
	fontB.Sz.Val = "11"
	fontB.Color.RGB = "12345678"
	c.Assert(fontA.Equals(fontB), Equals, false)
	fontB.Color.RGB = "FFFF0000"
	fontB.Name.Val = "Arial"
	c.Assert(fontA.Equals(fontB), Equals, false)
	fontB.Name.Val = "Calibri"
	fontB.Family.Val = "1"
	c.Assert(fontA.Equals(fontB), Equals, false)
	fontB.Family.Val = "2"
	fontB.B = nil
	c.Assert(fontA.Equals(fontB), Equals, false)
	fontB.B = &xlsxVal{}
	fontB.I = nil
	c.Assert(fontA.Equals(fontB), Equals, false)
	fontB.I = &xlsxVal{}
	fontB.U = nil
	c.Assert(fontA.Equals(fontB), Equals, false)
	fontB.U = &xlsxVal{}
	// For sanity
	c.Assert(fontA.Equals(fontB), Equals, true)
}

func (x *XMLStyleSuite) TestFillEquals(c *C) {
	fillA := xlsxFill{PatternFill: xlsxPatternFill{
		PatternType: "solid",
		FgColor:     xlsxColor{RGB: "FFFF0000"},
		BgColor:     xlsxColor{RGB: "0000FFFF"}}}
	fillB := xlsxFill{PatternFill: xlsxPatternFill{
		PatternType: "solid",
		FgColor:     xlsxColor{RGB: "FFFF0000"},
		BgColor:     xlsxColor{RGB: "0000FFFF"}}}
	c.Assert(fillA.Equals(fillB), Equals, true)
	fillB.PatternFill.PatternType = "gray125"
	c.Assert(fillA.Equals(fillB), Equals, false)
	fillB.PatternFill.PatternType = "solid"
	fillB.PatternFill.FgColor.RGB = "00FF00FF"
	c.Assert(fillA.Equals(fillB), Equals, false)
	fillB.PatternFill.FgColor.RGB = "FFFF0000"
	fillB.PatternFill.BgColor.RGB = "12456789"
	c.Assert(fillA.Equals(fillB), Equals, false)
	fillB.PatternFill.BgColor.RGB = "0000FFFF"
	// For sanity
	c.Assert(fillA.Equals(fillB), Equals, true)
}

func (x *XMLStyleSuite) TestBorderEquals(c *C) {
	borderA := xlsxBorder{Left: xlsxLine{Style: "none"},
		Right:  xlsxLine{Style: "none"},
		Top:    xlsxLine{Style: "none"},
		Bottom: xlsxLine{Style: "none"}}
	borderB := xlsxBorder{Left: xlsxLine{Style: "none"},
		Right:  xlsxLine{Style: "none"},
		Top:    xlsxLine{Style: "none"},
		Bottom: xlsxLine{Style: "none"}}
	c.Assert(borderA.Equals(borderB), Equals, true)
	borderB.Left.Style = "thin"
	c.Assert(borderA.Equals(borderB), Equals, false)
	borderB.Left.Style = "none"
	borderB.Right.Style = "thin"
	c.Assert(borderA.Equals(borderB), Equals, false)
	borderB.Right.Style = "none"
	borderB.Top.Style = "thin"
	c.Assert(borderA.Equals(borderB), Equals, false)
	borderB.Top.Style = "none"
	borderB.Bottom.Style = "thin"
	c.Assert(borderA.Equals(borderB), Equals, false)
	borderB.Bottom.Style = "none"
	// for sanity
	c.Assert(borderA.Equals(borderB), Equals, true)
}

func (x *XMLStyleSuite) TestXfEquals(c *C) {
	xfA := xlsxXf{
		ApplyAlignment:  true,
		ApplyBorder:     true,
		ApplyFont:       true,
		ApplyFill:       true,
		ApplyProtection: true,
		BorderId:        0,
		FillId:          0,
		FontId:          0,
		NumFmtId:        0}
	xfB := xlsxXf{
		ApplyAlignment:  true,
		ApplyBorder:     true,
		ApplyFont:       true,
		ApplyFill:       true,
		ApplyProtection: true,
		BorderId:        0,
		FillId:          0,
		FontId:          0,
		NumFmtId:        0}
	c.Assert(xfA.Equals(xfB), Equals, true)
	xfB.ApplyAlignment = false
	c.Assert(xfA.Equals(xfB), Equals, false)
	xfB.ApplyAlignment = true
	xfB.ApplyBorder = false
	c.Assert(xfA.Equals(xfB), Equals, false)
	xfB.ApplyBorder = true
	xfB.ApplyFont = false
	c.Assert(xfA.Equals(xfB), Equals, false)
	xfB.ApplyFont = true
	xfB.ApplyFill = false
	c.Assert(xfA.Equals(xfB), Equals, false)
	xfB.ApplyFill = true
	xfB.ApplyProtection = false
	c.Assert(xfA.Equals(xfB), Equals, false)
	xfB.ApplyProtection = true
	xfB.BorderId = 1
	c.Assert(xfA.Equals(xfB), Equals, false)
	xfB.BorderId = 0
	xfB.FillId = 1
	c.Assert(xfA.Equals(xfB), Equals, false)
	xfB.FillId = 0
	xfB.FontId = 1
	c.Assert(xfA.Equals(xfB), Equals, false)
	xfB.FontId = 0
	xfB.NumFmtId = 1
	c.Assert(xfA.Equals(xfB), Equals, false)
	xfB.NumFmtId = 0
	// for sanity
	c.Assert(xfA.Equals(xfB), Equals, true)
}

func (s *CellSuite) TestNewNumFmt(c *C) {
	styles := newXlsxStyleSheet(nil)
	styles.NumFmts = xlsxNumFmts{}
	styles.NumFmts.NumFmt = make([]xlsxNumFmt, 0)

	c.Assert(styles.newNumFmt("0"), DeepEquals, xlsxNumFmt{1, "0"})
	c.Assert(styles.newNumFmt("0.00e+00"), DeepEquals, xlsxNumFmt{11, "0.00e+00"})
	c.Assert(styles.newNumFmt("mm-dd-yy"), DeepEquals, xlsxNumFmt{14, "mm-dd-yy"})
	c.Assert(styles.newNumFmt("hh:mm:ss"), DeepEquals, xlsxNumFmt{164, "hh:mm:ss"})
	c.Assert(len(styles.NumFmts.NumFmt), Equals, 1)
}

func (s *CellSuite) TestAddNumFmt(c *C) {
	styles := &xlsxStyleSheet{}
	styles.NumFmts = xlsxNumFmts{}
	styles.NumFmts.NumFmt = make([]xlsxNumFmt, 0)

	styles.addNumFmt(xlsxNumFmt{1, "0"})
	c.Assert(styles.NumFmts.Count, Equals, 0)
	styles.addNumFmt(xlsxNumFmt{14, "mm-dd-yy"})
	c.Assert(styles.NumFmts.Count, Equals, 0)
	styles.addNumFmt(xlsxNumFmt{164, "hh:mm:ss"})
	c.Assert(styles.NumFmts.Count, Equals, 1)
	styles.addNumFmt(xlsxNumFmt{165, "yyyy/mm/dd"})
	c.Assert(styles.NumFmts.Count, Equals, 2)
	styles.addNumFmt(xlsxNumFmt{165, "yyyy/mm/dd"})
	c.Assert(styles.NumFmts.Count, Equals, 2)
}
