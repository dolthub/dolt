package xlsx

import (
	"encoding/xml"

	. "gopkg.in/check.v1"
)

type ContentTypesSuite struct{}

var _ = Suite(&ContentTypesSuite{})

func (l *ContentTypesSuite) TestMarshalContentTypes(c *C) {
	var types xlsxTypes = xlsxTypes{}
	types.Overrides = make([]xlsxOverride, 1)
	types.Overrides[0] = xlsxOverride{PartName: "/_rels/.rels", ContentType: "application/vnd.openxmlformats-package.relationships+xml"}
	output, err := xml.Marshal(types)
	stringOutput := xml.Header + string(output)
	c.Assert(err, IsNil)
	expectedContentTypes := `<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Override PartName="/_rels/.rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"></Override></Types>`
	c.Assert(stringOutput, Equals, expectedContentTypes)
}

func (l *ContentTypesSuite) TestMakeDefaultContentTypes(c *C) {
	var types xlsxTypes = MakeDefaultContentTypes()
	c.Assert(len(types.Overrides), Equals, 8)
	c.Assert(types.Overrides[0].PartName, Equals, "/_rels/.rels")
	c.Assert(types.Overrides[0].ContentType, Equals, "application/vnd.openxmlformats-package.relationships+xml")
	c.Assert(types.Overrides[1].PartName, Equals, "/docProps/app.xml")
	c.Assert(types.Overrides[1].ContentType, Equals, "application/vnd.openxmlformats-officedocument.extended-properties+xml")
	c.Assert(types.Overrides[2].PartName, Equals, "/docProps/core.xml")
	c.Assert(types.Overrides[2].ContentType, Equals, "application/vnd.openxmlformats-package.core-properties+xml")
	c.Assert(types.Overrides[3].PartName, Equals, "/xl/_rels/workbook.xml.rels")
	c.Assert(types.Overrides[3].ContentType, Equals, "application/vnd.openxmlformats-package.relationships+xml")
	c.Assert(types.Overrides[4].PartName, Equals, "/xl/sharedStrings.xml")
	c.Assert(types.Overrides[4].ContentType, Equals, "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml")
	c.Assert(types.Overrides[5].PartName, Equals, "/xl/styles.xml")
	c.Assert(types.Overrides[5].ContentType, Equals, "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml")
	c.Assert(types.Overrides[6].PartName, Equals, "/xl/workbook.xml")
	c.Assert(types.Overrides[6].ContentType, Equals, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml")
	c.Assert(types.Overrides[7].PartName, Equals, "/xl/theme/theme1.xml")
	c.Assert(types.Overrides[7].ContentType, Equals, "application/vnd.openxmlformats-officedocument.theme+xml")

	c.Assert(types.Defaults[0].Extension, Equals, "rels")
	c.Assert(types.Defaults[0].ContentType, Equals, "application/vnd.openxmlformats-package.relationships+xml")
	c.Assert(types.Defaults[1].Extension, Equals, "xml")
	c.Assert(types.Defaults[1].ContentType, Equals, "application/xml")

}
