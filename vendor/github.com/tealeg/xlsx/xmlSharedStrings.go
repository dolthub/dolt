package xlsx

import (
	"encoding/xml"
)

// xlsxSST directly maps the sst element from the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main currently
// I have not checked this for completeness - it does as much as I need.
type xlsxSST struct {
	XMLName     xml.Name `xml:"http://schemas.openxmlformats.org/spreadsheetml/2006/main sst"`
	Count       int      `xml:"count,attr"`
	UniqueCount int      `xml:"uniqueCount,attr"`
	SI          []xlsxSI `xml:"si"`
}

// xlsxSI directly maps the si element from the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked this for completeness - it does as
// much as I need.
type xlsxSI struct {
	T string  `xml:"t"`
	R []xlsxR `xml:"r"`
}

// xlsxR directly maps the r element from the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked this for completeness - it does as
// much as I need.
type xlsxR struct {
	T string `xml:"t"`
}
