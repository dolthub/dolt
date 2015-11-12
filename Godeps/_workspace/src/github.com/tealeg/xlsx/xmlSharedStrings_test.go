package xlsx

import (
	"bytes"
	"encoding/xml"

	. "gopkg.in/check.v1"
)

type SharedStringsSuite struct {
	SharedStringsXML *bytes.Buffer
}

var _ = Suite(&SharedStringsSuite{})

func (s *SharedStringsSuite) SetUpTest(c *C) {
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

// Test we can correctly unmarshal an the sharedstrings.xml file into
// an xlsx.xlsxSST struct and it's associated children.
func (s *SharedStringsSuite) TestUnmarshallSharedStrings(c *C) {
	sst := new(xlsxSST)
	err := xml.NewDecoder(s.SharedStringsXML).Decode(sst)
	c.Assert(err, IsNil)
	c.Assert(sst.Count, Equals, 4)
	c.Assert(sst.UniqueCount, Equals, 4)
	c.Assert(sst.SI, HasLen, 4)
	si := sst.SI[0]
	c.Assert(si.T, Equals, "Foo")
}
