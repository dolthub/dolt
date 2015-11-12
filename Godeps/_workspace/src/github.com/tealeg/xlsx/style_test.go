package xlsx

import (
	. "gopkg.in/check.v1"
)

type StyleSuite struct{}

var _ = Suite(&StyleSuite{})

func (s *StyleSuite) TestNewStyle(c *C) {
	style := NewStyle()
	c.Assert(style, NotNil)
}

func (s *StyleSuite) TestNewStyleDefaultts(c *C) {
	style := NewStyle()
	c.Assert(style.Font, Equals, *DefaultFont())
	c.Assert(style.Fill, Equals, *DefaultFill())
	c.Assert(style.Border, Equals, *DefaultBorder())
}

func (s *StyleSuite) TestMakeXLSXStyleElements(c *C) {
	style := NewStyle()
	font := *NewFont(12, "Verdana")
	font.Bold = true
	font.Italic = true
	font.Underline = true
	style.Font = font
	fill := *NewFill("solid", "00FF0000", "FF000000")
	style.Fill = fill
	border := *NewBorder("thin", "thin", "thin", "thin")
	style.Border = border
	style.ApplyBorder = true
	style.ApplyFill = true

	style.ApplyFont = true
	xFont, xFill, xBorder, xCellStyleXf, xCellXf := style.makeXLSXStyleElements()
	// c.Assert(xNumFmt.NumFmtId, Equals, 164)
	// c.Assert(xNumFmt.FormatCode, Equals, "GENERAL")
	c.Assert(xFont.Sz.Val, Equals, "12")
	c.Assert(xFont.Name.Val, Equals, "Verdana")
	c.Assert(xFont.B, NotNil)
	c.Assert(xFont.I, NotNil)
	c.Assert(xFont.U, NotNil)
	c.Assert(xFill.PatternFill.PatternType, Equals, "solid")
	c.Assert(xFill.PatternFill.FgColor.RGB, Equals, "00FF0000")
	c.Assert(xFill.PatternFill.BgColor.RGB, Equals, "FF000000")
	c.Assert(xBorder.Left.Style, Equals, "thin")
	c.Assert(xBorder.Right.Style, Equals, "thin")
	c.Assert(xBorder.Top.Style, Equals, "thin")
	c.Assert(xBorder.Bottom.Style, Equals, "thin")
	c.Assert(xCellStyleXf.ApplyBorder, Equals, true)
	c.Assert(xCellStyleXf.ApplyFill, Equals, true)
	c.Assert(xCellStyleXf.ApplyFont, Equals, true)
	c.Assert(xCellXf.ApplyBorder, Equals, true)
	c.Assert(xCellXf.ApplyFill, Equals, true)
	c.Assert(xCellXf.ApplyFont, Equals, true)

}

type FontSuite struct{}

var _ = Suite(&FontSuite{})

func (s *FontSuite) TestNewFont(c *C) {
	font := NewFont(12, "Verdana")
	c.Assert(font, NotNil)
	c.Assert(font.Name, Equals, "Verdana")
	c.Assert(font.Size, Equals, 12)
}
