package xlsx

import (
	"encoding/xml"
)

type xlsxTypes struct {
	XMLName xml.Name `xml:"http://schemas.openxmlformats.org/package/2006/content-types Types"`

	Overrides []xlsxOverride `xml:"Override"`
	Defaults  []xlsxDefault  `xml:"Default"`
}

type xlsxOverride struct {
	PartName    string `xml:",attr"`
	ContentType string `xml:",attr"`
}

type xlsxDefault struct {
	Extension   string `xml:",attr"`
	ContentType string `xml:",attr"`
}

func MakeDefaultContentTypes() (types xlsxTypes) {
	types.Overrides = make([]xlsxOverride, 8)
	types.Defaults = make([]xlsxDefault, 2)

	types.Overrides[0].PartName = "/_rels/.rels"
	types.Overrides[0].ContentType = "application/vnd.openxmlformats-package.relationships+xml"
	types.Overrides[1].PartName = "/docProps/app.xml"
	types.Overrides[1].ContentType = "application/vnd.openxmlformats-officedocument.extended-properties+xml"
	types.Overrides[2].PartName = "/docProps/core.xml"
	types.Overrides[2].ContentType = "application/vnd.openxmlformats-package.core-properties+xml"
	types.Overrides[3].PartName = "/xl/_rels/workbook.xml.rels"
	types.Overrides[3].ContentType = "application/vnd.openxmlformats-package.relationships+xml"
	types.Overrides[4].PartName = "/xl/sharedStrings.xml"
	types.Overrides[4].ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"
	types.Overrides[5].PartName = "/xl/styles.xml"
	types.Overrides[5].ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"
	types.Overrides[6].PartName = "/xl/workbook.xml"
	types.Overrides[6].ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"
	types.Overrides[7].PartName = "/xl/theme/theme1.xml"
	types.Overrides[7].ContentType = "application/vnd.openxmlformats-officedocument.theme+xml"

	types.Defaults[0].Extension = "rels"
	types.Defaults[0].ContentType = "application/vnd.openxmlformats-package.relationships+xml"
	types.Defaults[1].Extension = "xml"
	types.Defaults[1].ContentType = "application/xml"
	return
}
