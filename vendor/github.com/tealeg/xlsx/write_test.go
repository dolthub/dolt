package xlsx

import (
	. "gopkg.in/check.v1"
)

type WriteSuite struct{}

var _ = Suite(&WriteSuite{})

// Test if we can write a struct to a row
func (r *RowSuite) TestWriteStruct(c *C) {
	var f *File
	f = NewFile()
	sheet, _ := f.AddSheet("Test1")
	row := sheet.AddRow()
	type e struct {
		FirstName string
		Age       int
		GPA       float64
		LikesPHP  bool
	}
	testStruct := e{
		"Eric",
		20,
		3.94,
		false,
	}
	row.WriteStruct(&testStruct, -1)
	c.Assert(row, NotNil)

	c0 := row.Cells[0].String()
	c1, e1 := row.Cells[1].Int()
	c2, e2 := row.Cells[2].Float()
	c3 := row.Cells[3].Bool()

	c.Assert(c0, Equals, "Eric")
	c.Assert(c1, Equals, 20)
	c.Assert(c2, Equals, 3.94)
	c.Assert(c3, Equals, false)

	c.Assert(e1, Equals, nil)
	c.Assert(e2, Equals, nil)
}

// Test if we can write a slice to a row
func (r *RowSuite) TestWriteSlice(c *C) {
	var f *File
	f = NewFile()
	sheet, _ := f.AddSheet("Test1")

	type strA []string
	type intA []int
	type floatA []float64
	type boolA []bool
	type interfaceA []interface{}

	s0 := strA{"Eric"}
	row0 := sheet.AddRow()
	row0.WriteSlice(&s0, -1)
	c.Assert(row0, NotNil)
	c0 := row0.Cells[0].String()
	c.Assert(c0, Equals, "Eric")

	s1 := intA{10}
	row1 := sheet.AddRow()
	row1.WriteSlice(&s1, -1)
	c.Assert(row1, NotNil)
	c1, e1 := row1.Cells[0].Int()
	c.Assert(e1, Equals, nil)
	c.Assert(c1, Equals, 10)

	s2 := floatA{3.94}
	row2 := sheet.AddRow()
	row2.WriteSlice(&s2, -1)
	c.Assert(row2, NotNil)
	c2, e2 := row2.Cells[0].Float()
	c.Assert(e2, Equals, nil)
	c.Assert(c2, Equals, 3.94)

	s3 := boolA{true}
	row3 := sheet.AddRow()
	row3.WriteSlice(&s3, -1)
	c.Assert(row3, NotNil)
	c3 := row3.Cells[0].Bool()
	c.Assert(c3, Equals, true)

	s4 := interfaceA{"Eric", 10, 3.94, true}
	row4 := sheet.AddRow()
	row4.WriteSlice(&s4, -1)
	c.Assert(row4, NotNil)
	c40 := row4.Cells[0].String()
	c.Assert(c40, Equals, "Eric")
	c41, e41 := row4.Cells[1].Int()
	c.Assert(e41, Equals, nil)
	c.Assert(c41, Equals, 10)
	c42, e42 := row4.Cells[2].Float()
	c.Assert(e42, Equals, nil)
	c.Assert(c42, Equals, 3.94)
	c43 := row4.Cells[3].Bool()
	c.Assert(c43, Equals, true)
}
