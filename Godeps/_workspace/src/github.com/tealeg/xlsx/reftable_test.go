package xlsx

import (
	"bytes"
	"encoding/xml"

	. "gopkg.in/check.v1"
)

type RefTableSuite struct {
	SharedStringsXML *bytes.Buffer
}

var _ = Suite(&RefTableSuite{})

func (s *RefTableSuite) SetUpTest(c *C) {
	s.SharedStringsXML = bytes.NewBufferString(
		`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
             count="4"
             uniqueCount="4">
          <si>
            <t>Foo</t>
          </si>
          <si>
            <t>Bar</t>
          </si>
          <si>
            <t xml:space="preserve">Baz </t>
          </si>
          <si>
            <t>Quuk</t>
          </si>
        </sst>`)
}

// We can add a new string to the RefTable
func (s *RefTableSuite) TestRefTableAddString(c *C) {
	refTable := NewSharedStringRefTable()
	index := refTable.AddString("Foo")
	c.Assert(index, Equals, 0)
	c.Assert(refTable.ResolveSharedString(0), Equals, "Foo")
}

func (s *RefTableSuite) TestCreateNewSharedStringRefTable(c *C) {
	refTable := NewSharedStringRefTable()
	refTable.AddString("Foo")
	refTable.AddString("Bar")
	c.Assert(refTable.ResolveSharedString(0), Equals, "Foo")
	c.Assert(refTable.ResolveSharedString(1), Equals, "Bar")
}

// Test we can correctly convert a xlsxSST into a reference table
// using xlsx.MakeSharedStringRefTable().
func (s *RefTableSuite) TestMakeSharedStringRefTable(c *C) {
	sst := new(xlsxSST)
	err := xml.NewDecoder(s.SharedStringsXML).Decode(sst)
	c.Assert(err, IsNil)
	reftable := MakeSharedStringRefTable(sst)
	c.Assert(reftable.Length(), Equals, 4)
	c.Assert(reftable.ResolveSharedString(0), Equals, "Foo")
	c.Assert(reftable.ResolveSharedString(1), Equals, "Bar")
}

// Test we can correctly resolve a numeric reference in the reference
// table to a string value using RefTable.ResolveSharedString().
func (s *RefTableSuite) TestResolveSharedString(c *C) {
	sst := new(xlsxSST)
	err := xml.NewDecoder(s.SharedStringsXML).Decode(sst)
	c.Assert(err, IsNil)
	reftable := MakeSharedStringRefTable(sst)
	c.Assert(reftable.ResolveSharedString(0), Equals, "Foo")
}

// Test we can correctly create the xlsx.xlsxSST struct from a RefTable
func (s *RefTableSuite) TestMakeXLSXSST(c *C) {
	refTable := NewSharedStringRefTable()
	refTable.AddString("Foo")
	refTable.AddString("Bar")
	sst := refTable.makeXLSXSST()
	c.Assert(sst, NotNil)
	c.Assert(sst.Count, Equals, 2)
	c.Assert(sst.UniqueCount, Equals, 2)
	c.Assert(sst.SI, HasLen, 2)
	si := sst.SI[0]
	c.Assert(si.T, Equals, "Foo")
}

func (s *RefTableSuite) TestMarshalSST(c *C) {
	refTable := NewSharedStringRefTable()
	refTable.AddString("Foo")
	sst := refTable.makeXLSXSST()

	output := bytes.NewBufferString(xml.Header)
	body, err := xml.Marshal(sst)
	c.Assert(err, IsNil)
	c.Assert(body, NotNil)
	_, err = output.Write(body)
	c.Assert(err, IsNil)

	expectedXLSXSST := `<?xml version="1.0" encoding="UTF-8"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="1" uniqueCount="1"><si><t>Foo</t></si></sst>`
	c.Assert(output.String(), Equals, expectedXLSXSST)
}

func (s *RefTableSuite) TestRefTableReadAddString(c *C) {
	refTable := NewSharedStringRefTable()
	refTable.isWrite = false
	index1 := refTable.AddString("Foo")
	index2 := refTable.AddString("Foo")
	c.Assert(index1, Equals, 0)
	c.Assert(index2, Equals, 1)
	c.Assert(refTable.ResolveSharedString(0), Equals, "Foo")
	c.Assert(refTable.ResolveSharedString(1), Equals, "Foo")
}

func (s *RefTableSuite) TestRefTableWriteAddString(c *C) {
	refTable := NewSharedStringRefTable()
	refTable.isWrite = true
	index1 := refTable.AddString("Foo")
	index2 := refTable.AddString("Foo")
	c.Assert(index1, Equals, 0)
	c.Assert(index2, Equals, 0)
	c.Assert(refTable.ResolveSharedString(0), Equals, "Foo")
}
