package xlsx

import (
	. "gopkg.in/check.v1"
)

type RowSuite struct{}

var _ = Suite(&RowSuite{})

// Test we can add a new Cell to a Row
func (r *RowSuite) TestAddCell(c *C) {
	var f *File
	f = NewFile()
	sheet, _ := f.AddSheet("MySheet")
	row := sheet.AddRow()
	cell := row.AddCell()
	c.Assert(cell, NotNil)
	c.Assert(len(row.Cells), Equals, 1)
}
