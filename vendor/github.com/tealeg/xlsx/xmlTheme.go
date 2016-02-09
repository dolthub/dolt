package xlsx

import "encoding/xml"

// xlsxTheme directly maps the theme element in the namespace
// http://schemas.openxmlformats.org/drawingml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxTheme struct {
	ThemeElements xlsxThemeElements `xml:"themeElements"`
}

// xlsxThemeElements directly maps the themeElements element in the namespace
// http://schemas.openxmlformats.org/drawingml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxThemeElements struct {
	ClrScheme xlsxClrScheme `xml:"clrScheme"`
}

// xlsxClrScheme directly maps the clrScheme element in the namespace
// http://schemas.openxmlformats.org/drawingml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxClrScheme struct {
	Name     string            `xml:"name,attr"`
	Children []xlsxClrSchemeEl `xml:",any"`
}

// xlsxClrScheme maps to children of the clrScheme element in the namespace
// http://schemas.openxmlformats.org/drawingml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxClrSchemeEl struct {
	XMLName xml.Name
	SysClr  *xlsxSysClr  `xml:"sysClr"`
	SrgbClr *xlsxSrgbClr `xml:"srgbClr"`
}

// xlsxSysClr directly maps the sysClr element in the namespace
// http://schemas.openxmlformats.org/drawingml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxSysClr struct {
	Val     string `xml:"val,attr"`
	LastClr string `xml:"lastClr,attr"`
}

// xlsxSrgbClr directly maps the srgbClr element in the namespace
// http://schemas.openxmlformats.org/drawingml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxSrgbClr struct {
	Val string `xml:"val,attr"`
}
