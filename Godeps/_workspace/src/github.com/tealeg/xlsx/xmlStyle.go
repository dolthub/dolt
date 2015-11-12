// xslx is a package designed to help with reading data from
// spreadsheets stored in the XLSX format used in recent versions of
// Microsoft's Excel spreadsheet.
//
// For a concise example of how to use this library why not check out
// the source for xlsx2csv here: https://github.com/tealeg/xlsx2csv

package xlsx

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Excel styles can reference number formats that are built-in, all of which
// have an id less than 164.
const builtinNumFmtsCount = 163

// Excel styles can reference number formats that are built-in, all of which
// have an id less than 164. This is a possibly incomplete list comprised of as
// many of them as I could find.
var builtInNumFmt = map[int]string{
	0:  "general",
	1:  "0",
	2:  "0.00",
	3:  "#,##0",
	4:  "#,##0.00",
	9:  "0%",
	10: "0.00%",
	11: "0.00e+00",
	12: "# ?/?",
	13: "# ??/??",
	14: "mm-dd-yy",
	15: "d-mmm-yy",
	16: "d-mmm",
	17: "mmm-yy",
	18: "h:mm am/pm",
	19: "h:mm:ss am/pm",
	20: "h:mm",
	21: "h:mm:ss",
	22: "m/d/yy h:mm",
	37: "#,##0 ;(#,##0)",
	38: "#,##0 ;[red](#,##0)",
	39: "#,##0.00;(#,##0.00)",
	40: "#,##0.00;[red](#,##0.00)",
	41: `_(* #,##0_);_(* \(#,##0\);_(* "-"_);_(@_)`,
	42: `_("$"* #,##0_);_("$* \(#,##0\);_("$"* "-"_);_(@_)`,
	43: `_(* #,##0.00_);_(* \(#,##0.00\);_(* "-"??_);_(@_)`,
	44: `_("$"* #,##0.00_);_("$"* \(#,##0.00\);_("$"* "-"??_);_(@_)`,
	45: "mm:ss",
	46: "[h]:mm:ss",
	47: "mmss.0",
	48: "##0.0e+0",
	49: "@",
}

const (
	builtInNumFmtIndex_GENERAL = int(0)
	builtInNumFmtIndex_INT     = int(1)
	builtInNumFmtIndex_FLOAT   = int(2)
	builtInNumFmtIndex_DATE    = int(14)
	builtInNumFmtIndex_STRING  = int(49)
)

// xlsxStyle directly maps the styleSheet element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxStyleSheet struct {
	XMLName xml.Name `xml:"http://schemas.openxmlformats.org/spreadsheetml/2006/main styleSheet"`

	Fonts        xlsxFonts        `xml:"fonts,omitempty"`
	Fills        xlsxFills        `xml:"fills,omitempty"`
	Borders      xlsxBorders      `xml:"borders,omitempty"`
	CellStyleXfs xlsxCellStyleXfs `xml:"cellStyleXfs,omitempty"`
	CellXfs      xlsxCellXfs      `xml:"cellXfs,omitempty"`
	NumFmts      xlsxNumFmts      `xml:"numFmts,omitempty"`

	theme          *theme
	styleCache     map[int]*Style
	numFmtRefTable map[int]xlsxNumFmt
	lock           *sync.RWMutex
}

func newXlsxStyleSheet(t *theme) *xlsxStyleSheet {
	stylesheet := new(xlsxStyleSheet)
	stylesheet.theme = t
	stylesheet.styleCache = make(map[int]*Style)
	stylesheet.lock = new(sync.RWMutex)
	return stylesheet
}

func (styles *xlsxStyleSheet) reset() {
	styles.Fonts = xlsxFonts{}
	styles.Fills = xlsxFills{}
	styles.Borders = xlsxBorders{}
	styles.CellStyleXfs = xlsxCellStyleXfs{}
	// add default xf
	styles.CellXfs = xlsxCellXfs{Count: 1, Xf: []xlsxXf{xlsxXf{}}}
	styles.NumFmts = xlsxNumFmts{}
}

func (styles *xlsxStyleSheet) getStyle(styleIndex int) (style *Style) {
	styles.lock.RLock()
	style, ok := styles.styleCache[styleIndex]
	styles.lock.RUnlock()
	if ok {
		return
	}
	var styleXf xlsxXf
	style = &Style{}
	style.Border = Border{}
	style.Fill = Fill{}
	style.Font = Font{}

	xfCount := styles.CellXfs.Count
	if styleIndex > -1 && xfCount > 0 && styleIndex <= xfCount {
		xf := styles.CellXfs.Xf[styleIndex]

		// Google docs can produce output that has fewer
		// CellStyleXfs than CellXfs - this copes with that.
		if styleIndex < styles.CellStyleXfs.Count {
			styleXf = styles.CellStyleXfs.Xf[styleIndex]
		} else {
			styleXf = xlsxXf{}
		}

		style.ApplyBorder = xf.ApplyBorder || styleXf.ApplyBorder
		style.ApplyFill = xf.ApplyFill || styleXf.ApplyFill
		style.ApplyFont = xf.ApplyFont || styleXf.ApplyFont
		style.ApplyAlignment = xf.ApplyAlignment || styleXf.ApplyAlignment

		if xf.BorderId > -1 && xf.BorderId < styles.Borders.Count {
			var border xlsxBorder
			border = styles.Borders.Border[xf.BorderId]
			style.Border.Left = border.Left.Style
			style.Border.LeftColor = border.Left.Color.RGB
			style.Border.Right = border.Right.Style
			style.Border.RightColor = border.Right.Color.RGB
			style.Border.Top = border.Top.Style
			style.Border.TopColor = border.Top.Color.RGB
			style.Border.Bottom = border.Bottom.Style
			style.Border.BottomColor = border.Bottom.Color.RGB
		}

		if xf.FillId > -1 && xf.FillId < styles.Fills.Count {
			xFill := styles.Fills.Fill[xf.FillId]
			style.Fill.PatternType = xFill.PatternFill.PatternType
			style.Fill.FgColor = styles.argbValue(xFill.PatternFill.FgColor)
			style.Fill.BgColor = styles.argbValue(xFill.PatternFill.BgColor)
		}

		if xf.FontId > -1 && xf.FontId < styles.Fonts.Count {
			xfont := styles.Fonts.Font[xf.FontId]
			style.Font.Size, _ = strconv.Atoi(xfont.Sz.Val)
			style.Font.Name = xfont.Name.Val
			style.Font.Family, _ = strconv.Atoi(xfont.Family.Val)
			style.Font.Charset, _ = strconv.Atoi(xfont.Charset.Val)
			style.Font.Color = styles.argbValue(xfont.Color)

			if bold := xfont.B; bold != nil && bold.Val != "0" {
				style.Font.Bold = true
			}
			if italic := xfont.I; italic != nil && italic.Val != "0" {
				style.Font.Italic = true
			}
			if underline := xfont.U; underline != nil && underline.Val != "0" {
				style.Font.Underline = true
			}
		}
		if xf.Alignment.Horizontal != "" {
			style.Alignment.Horizontal = xf.Alignment.Horizontal
		}

		if xf.Alignment.Vertical != "" {
			style.Alignment.Vertical = xf.Alignment.Vertical
		}
		styles.lock.Lock()
		styles.styleCache[styleIndex] = style
		styles.lock.Unlock()
	}
	return style
}

func (styles *xlsxStyleSheet) argbValue(color xlsxColor) string {
	if color.Theme != nil && styles.theme != nil {
		return styles.theme.themeColor(int64(*color.Theme), color.Tint)
	} else {
		return color.RGB
	}
}

// Excel styles can reference number formats that are built-in, all of which
// have an id less than 164. This is a possibly incomplete list comprised of as
// many of them as I could find.
func getBuiltinNumberFormat(numFmtId int) string {
	return builtInNumFmt[numFmtId]
}

func (styles *xlsxStyleSheet) getNumberFormat(styleIndex int) string {
	if styles.CellXfs.Xf == nil {
		return ""
	}
	var numberFormat string = ""
	if styleIndex > -1 && styleIndex <= styles.CellXfs.Count {
		xf := styles.CellXfs.Xf[styleIndex]
		if builtin := getBuiltinNumberFormat(xf.NumFmtId); builtin != "" {
			return builtin
		}
		if styles.numFmtRefTable != nil {
			numFmt := styles.numFmtRefTable[xf.NumFmtId]
			numberFormat = numFmt.FormatCode
		}
	}
	return strings.ToLower(numberFormat)
}

func (styles *xlsxStyleSheet) addFont(xFont xlsxFont) (index int) {
	var font xlsxFont
	if xFont.Name.Val == "" {
		return 0
	}
	for index, font = range styles.Fonts.Font {
		if font.Equals(xFont) {
			return index
		}
	}
	styles.Fonts.Font = append(styles.Fonts.Font, xFont)
	index = styles.Fonts.Count
	styles.Fonts.Count += 1
	return
}

func (styles *xlsxStyleSheet) addFill(xFill xlsxFill) (index int) {
	var fill xlsxFill
	for index, fill = range styles.Fills.Fill {
		if fill.Equals(xFill) {
			return index
		}
	}
	styles.Fills.Fill = append(styles.Fills.Fill, xFill)
	index = styles.Fills.Count
	styles.Fills.Count += 1
	return
}

func (styles *xlsxStyleSheet) addBorder(xBorder xlsxBorder) (index int) {
	var border xlsxBorder
	for index, border = range styles.Borders.Border {
		if border.Equals(xBorder) {
			return index
		}
	}
	styles.Borders.Border = append(styles.Borders.Border, xBorder)
	index = styles.Borders.Count
	styles.Borders.Count += 1
	return
}

func (styles *xlsxStyleSheet) addCellStyleXf(xCellStyleXf xlsxXf) (index int) {
	var cellStyleXf xlsxXf
	for index, cellStyleXf = range styles.CellStyleXfs.Xf {
		if cellStyleXf.Equals(xCellStyleXf) {
			return index
		}
	}
	styles.CellStyleXfs.Xf = append(styles.CellStyleXfs.Xf, xCellStyleXf)
	index = styles.CellStyleXfs.Count
	styles.CellStyleXfs.Count += 1
	return
}

func (styles *xlsxStyleSheet) addCellXf(xCellXf xlsxXf) (index int) {
	var cellXf xlsxXf
	for index, cellXf = range styles.CellXfs.Xf {
		if cellXf.Equals(xCellXf) {
			return index
		}
	}

	styles.CellXfs.Xf = append(styles.CellXfs.Xf, xCellXf)
	index = styles.CellXfs.Count
	styles.CellXfs.Count += 1
	return
}

// newNumFmt generate a xlsxNumFmt according the format code. When the FormatCode is built in, it will return a xlsxNumFmt with the NumFmtId defined in ECMA document, otherwise it will generate a new NumFmtId greater than 164.
func (styles *xlsxStyleSheet) newNumFmt(formatCode string) xlsxNumFmt {
	if formatCode == "" {
		return xlsxNumFmt{NumFmtId: 0, FormatCode: "general"}
	}
	// built in NumFmts in xmlStyle.go, traverse from the const.
	numFmts := make(map[string]int)
	for k, v := range builtInNumFmt {
		numFmts[v] = k
	}
	numFmtId, ok := numFmts[formatCode]
	if ok {
		return xlsxNumFmt{NumFmtId: numFmtId, FormatCode: formatCode}
	}

	// find the exist xlsxNumFmt
	for _, numFmt := range styles.NumFmts.NumFmt {
		if formatCode == numFmt.FormatCode {
			return numFmt
		}
	}

	// The user define NumFmtId. The one less than 164 in built in.
	numFmtId = builtinNumFmtsCount + 1
	styles.lock.Lock()
	defer styles.lock.Unlock()
	for {
		// get a unused NumFmtId
		if _, ok = styles.numFmtRefTable[numFmtId]; ok {
			numFmtId += 1
		} else {
			styles.addNumFmt(xlsxNumFmt{NumFmtId: numFmtId, FormatCode: formatCode})
			break
		}
	}
	return xlsxNumFmt{NumFmtId: numFmtId, FormatCode: formatCode}
}

// addNumFmt add xlsxNumFmt if its not exist.
func (styles *xlsxStyleSheet) addNumFmt(xNumFmt xlsxNumFmt) {
	// don't add built in NumFmt
	if xNumFmt.NumFmtId <= builtinNumFmtsCount {
		return
	}
	_, ok := styles.numFmtRefTable[xNumFmt.NumFmtId]
	if !ok {
		if styles.numFmtRefTable == nil {
			styles.numFmtRefTable = make(map[int]xlsxNumFmt)
		}
		styles.NumFmts.NumFmt = append(styles.NumFmts.NumFmt, xNumFmt)
		styles.numFmtRefTable[xNumFmt.NumFmtId] = xNumFmt
		styles.NumFmts.Count += 1
	}
}

func (styles *xlsxStyleSheet) Marshal() (result string, err error) {
	var xNumFmts string
	var xfonts string
	var xfills string
	var xborders string
	var xcellStyleXfs string
	var xcellXfs string

	var outputFontMap map[int]int = make(map[int]int)
	var outputFillMap map[int]int = make(map[int]int)
	var outputBorderMap map[int]int = make(map[int]int)

	result = xml.Header
	result += `<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">`

	xNumFmts, err = styles.NumFmts.Marshal()
	if err != nil {
		return
	}
	result += xNumFmts

	xfonts, err = styles.Fonts.Marshal(outputFontMap)
	if err != nil {
		return
	}
	result += xfonts

	xfills, err = styles.Fills.Marshal(outputFillMap)
	if err != nil {
		return
	}
	result += xfills

	xborders, err = styles.Borders.Marshal(outputBorderMap)
	if err != nil {
		return
	}
	result += xborders

	xcellStyleXfs, err = styles.CellStyleXfs.Marshal(outputBorderMap, outputFillMap, outputFontMap)
	if err != nil {
		return
	}
	result += xcellStyleXfs

	xcellXfs, err = styles.CellXfs.Marshal(outputBorderMap, outputFillMap, outputFontMap)
	if err != nil {
		return
	}
	result += xcellXfs

	result += `</styleSheet>`
	return
}

// xlsxNumFmts directly maps the numFmts element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxNumFmts struct {
	Count  int          `xml:"count,attr"`
	NumFmt []xlsxNumFmt `xml:"numFmt,omitempty"`
}

func (numFmts *xlsxNumFmts) Marshal() (result string, err error) {
	if numFmts.Count > 0 {
		result = fmt.Sprintf(`<numFmts count="%d">`, numFmts.Count)
		for _, numFmt := range numFmts.NumFmt {
			var xNumFmt string
			xNumFmt, err = numFmt.Marshal()
			if err != nil {
				return
			}
			result += xNumFmt
		}
		result += `</numFmts>`
	}
	return
}

// xlsxNumFmt directly maps the numFmt element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxNumFmt struct {
	NumFmtId   int    `xml:"numFmtId,attr,omitempty"`
	FormatCode string `xml:"formatCode,attr,omitempty"`
}

func (numFmt *xlsxNumFmt) Marshal() (result string, err error) {
	return fmt.Sprintf(`<numFmt numFmtId="%d" formatCode="%s"/>`, numFmt.NumFmtId, numFmt.FormatCode), nil
}

// xlsxFonts directly maps the fonts element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxFonts struct {
	XMLName xml.Name `xml:"fonts"`

	Count int        `xml:"count,attr"`
	Font  []xlsxFont `xml:"font,omitempty"`
}

func (fonts *xlsxFonts) Marshal(outputFontMap map[int]int) (result string, err error) {
	emittedCount := 0
	subparts := ""

	for i, font := range fonts.Font {
		var xfont string
		xfont, err = font.Marshal()
		if err != nil {
			return
		}
		if xfont != "" {
			outputFontMap[i] = emittedCount
			emittedCount += 1
			subparts += xfont
		}
	}
	if emittedCount > 0 {
		result = fmt.Sprintf(`<fonts count="%d">`, fonts.Count)
		result += subparts
		result += `</fonts>`
	}
	return
}

// xlsxFont directly maps the font element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxFont struct {
	Sz      xlsxVal   `xml:"sz,omitempty"`
	Name    xlsxVal   `xml:"name,omitempty"`
	Family  xlsxVal   `xml:"family,omitempty"`
	Charset xlsxVal   `xml:"charset,omitempty"`
	Color   xlsxColor `xml:"color,omitempty"`
	B       *xlsxVal  `xml:"b,omitempty"`
	I       *xlsxVal  `xml:"i,omitempty"`
	U       *xlsxVal  `xml:"u,omitempty"`
}

func (font *xlsxFont) Equals(other xlsxFont) bool {
	if (font.B == nil && other.B != nil) || (font.B != nil && other.B == nil) {
		return false
	}
	if (font.I == nil && other.I != nil) || (font.I != nil && other.I == nil) {
		return false
	}
	if (font.U == nil && other.U != nil) || (font.U != nil && other.U == nil) {
		return false
	}
	return font.Sz.Equals(other.Sz) && font.Name.Equals(other.Name) && font.Family.Equals(other.Family) && font.Charset.Equals(other.Charset) && font.Color.Equals(other.Color)
}

func (font *xlsxFont) Marshal() (result string, err error) {
	result = `<font>`
	if font.Sz.Val != "" {
		result += fmt.Sprintf(`<sz val="%s"/>`, font.Sz.Val)
	}
	if font.Name.Val != "" {
		result += fmt.Sprintf(`<name val="%s"/>`, font.Name.Val)
	}
	if font.Family.Val != "" {
		result += fmt.Sprintf(`<family val="%s"/>`, font.Family.Val)
	}
	if font.Charset.Val != "" {
		result += fmt.Sprintf(`<charset val="%s"/>`, font.Charset.Val)
	}
	if font.Color.RGB != "" {
		result += fmt.Sprintf(`<color rgb="%s"/>`, font.Color.RGB)
	}
	if font.B != nil {
		result += "<b/>"
	}
	if font.I != nil {
		result += "<i/>"
	}
	if font.U != nil {
		result += "<u/>"
	}
	result += `</font>`
	return
}

// xlsxVal directly maps the val element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxVal struct {
	Val string `xml:"val,attr,omitempty"`
}

func (val *xlsxVal) Equals(other xlsxVal) bool {
	return val.Val == other.Val
}

// xlsxFills directly maps the fills element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxFills struct {
	Count int        `xml:"count,attr"`
	Fill  []xlsxFill `xml:"fill,omitempty"`
}

func (fills *xlsxFills) Marshal(outputFillMap map[int]int) (result string, err error) {
	emittedCount := 0
	subparts := ""
	for i, fill := range fills.Fill {
		var xfill string
		xfill, err = fill.Marshal()
		if err != nil {
			return
		}
		if xfill != "" {
			outputFillMap[i] = emittedCount
			emittedCount += 1
			subparts += xfill
		}
	}
	if emittedCount > 0 {
		result = fmt.Sprintf(`<fills count="%d">`, emittedCount)
		result += subparts
		result += `</fills>`
	}
	return
}

// xlsxFill directly maps the fill element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxFill struct {
	PatternFill xlsxPatternFill `xml:"patternFill,omitempty"`
}

func (fill *xlsxFill) Equals(other xlsxFill) bool {
	return fill.PatternFill.Equals(other.PatternFill)
}

func (fill *xlsxFill) Marshal() (result string, err error) {
	if fill.PatternFill.PatternType != "" {
		var xpatternFill string
		result = `<fill>`

		xpatternFill, err = fill.PatternFill.Marshal()
		if err != nil {
			return
		}
		result += xpatternFill
		result += `</fill>`
	}
	return
}

// xlsxPatternFill directly maps the patternFill element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxPatternFill struct {
	PatternType string    `xml:"patternType,attr,omitempty"`
	FgColor     xlsxColor `xml:"fgColor,omitempty"`
	BgColor     xlsxColor `xml:"bgColor,omitempty"`
}

func (patternFill *xlsxPatternFill) Equals(other xlsxPatternFill) bool {
	return patternFill.PatternType == other.PatternType && patternFill.FgColor.Equals(other.FgColor) && patternFill.BgColor.Equals(other.BgColor)
}

func (patternFill *xlsxPatternFill) Marshal() (result string, err error) {
	result = fmt.Sprintf(`<patternFill patternType="%s"`, patternFill.PatternType)
	ending := `/>`
	terminator := ""
	subparts := ""
	if patternFill.FgColor.RGB != "" {
		ending = `>`
		terminator = "</patternFill>"
		subparts += fmt.Sprintf(`<fgColor rgb="%s"/>`, patternFill.FgColor.RGB)
	}
	if patternFill.BgColor.RGB != "" {
		ending = `>`
		terminator = "</patternFill>"
		subparts += fmt.Sprintf(`<bgColor rgb="%s"/>`, patternFill.BgColor.RGB)
	}
	result += ending
	result += subparts
	result += terminator
	return
}

// xlsxColor is a common mapping used for both the fgColor and bgColor
// elements in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxColor struct {
	RGB   string  `xml:"rgb,attr,omitempty"`
	Theme *int    `xml:"theme,attr,omitempty"`
	Tint  float64 `xml:"tint,attr,omitempty"`
}

func (color *xlsxColor) Equals(other xlsxColor) bool {
	return color.RGB == other.RGB
}

// xlsxBorders directly maps the borders element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxBorders struct {
	Count  int          `xml:"count,attr"`
	Border []xlsxBorder `xml:"border,omitempty"`
}

func (borders *xlsxBorders) Marshal(outputBorderMap map[int]int) (result string, err error) {
	result = ""
	emittedCount := 0
	subparts := ""
	for i, border := range borders.Border {
		var xborder string
		xborder, err = border.Marshal()
		if err != nil {
			return
		}
		if xborder != "" {
			outputBorderMap[i] = emittedCount
			emittedCount += 1
			subparts += xborder
		}
	}
	if emittedCount > 0 {
		result += fmt.Sprintf(`<borders count="%d">`, emittedCount)
		result += subparts
		result += `</borders>`
	}
	return
}

// xlsxBorder directly maps the border element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxBorder struct {
	Left   xlsxLine `xml:"left,omitempty"`
	Right  xlsxLine `xml:"right,omitempty"`
	Top    xlsxLine `xml:"top,omitempty"`
	Bottom xlsxLine `xml:"bottom,omitempty"`
}

func (border *xlsxBorder) Equals(other xlsxBorder) bool {
	return border.Left.Equals(other.Left) && border.Right.Equals(other.Right) && border.Top.Equals(other.Top) && border.Bottom.Equals(other.Bottom)
}

func (border *xlsxBorder) Marshal() (result string, err error) {
	emit := false
	subparts := ""
	if border.Left.Style != "" {
		emit = true
		subparts += fmt.Sprintf(`<left style="%s">`, border.Left.Style)
		if border.Left.Color.RGB != "" {
			subparts += fmt.Sprintf(`<color rgb="%s"/>`, border.Left.Color.RGB)
		}
		subparts += `</left>`
	}
	if border.Right.Style != "" {
		emit = true
		subparts += fmt.Sprintf(`<right style="%s">`, border.Right.Style)
		if border.Right.Color.RGB != "" {
			subparts += fmt.Sprintf(`<color rgb="%s"/>`, border.Right.Color.RGB)
		}
		subparts += `</right>`
	}
	if border.Top.Style != "" {
		emit = true
		subparts += fmt.Sprintf(`<top style="%s">`, border.Top.Style)
		if border.Top.Color.RGB != "" {
			subparts += fmt.Sprintf(`<color rgb="%s"/>`, border.Top.Color.RGB)
		}
		subparts += `</top>`
	}
	if border.Bottom.Style != "" {
		emit = true
		subparts += fmt.Sprintf(`<bottom style="%s">`, border.Bottom.Style)
		if border.Bottom.Color.RGB != "" {
			subparts += fmt.Sprintf(`<color rgb="%s"/>`, border.Bottom.Color.RGB)
		}
		subparts += `</bottom>`
	}
	if emit {
		result += `<border>`
		result += subparts
		result += `</border>`
	}
	return
}

// xlsxLine directly maps the line style element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxLine struct {
	Style string    `xml:"style,attr,omitempty"`
	Color xlsxColor `xml:"color,omitempty"`
}

func (line *xlsxLine) Equals(other xlsxLine) bool {
	return line.Style == other.Style && line.Color.Equals(other.Color)
}

// xlsxCellStyleXfs directly maps the cellStyleXfs element in the
// namespace http://schemas.openxmlformats.org/spreadsheetml/2006/main
// - currently I have not checked it for completeness - it does as
// much as I need.
type xlsxCellStyleXfs struct {
	Count int      `xml:"count,attr"`
	Xf    []xlsxXf `xml:"xf,omitempty"`
}

func (cellStyleXfs *xlsxCellStyleXfs) Marshal(outputBorderMap, outputFillMap, outputFontMap map[int]int) (result string, err error) {
	if cellStyleXfs.Count > 0 {
		result = fmt.Sprintf(`<cellStyleXfs count="%d">`, cellStyleXfs.Count)
		for _, xf := range cellStyleXfs.Xf {
			var xxf string
			xxf, err = xf.Marshal(outputBorderMap, outputFillMap, outputFontMap)
			if err != nil {
				return
			}
			result += xxf
		}
		result += `</cellStyleXfs>`
	}
	return
}

// xlsxCellXfs directly maps the cellXfs element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxCellXfs struct {
	Count int      `xml:"count,attr"`
	Xf    []xlsxXf `xml:"xf,omitempty"`
}

func (cellXfs *xlsxCellXfs) Marshal(outputBorderMap, outputFillMap, outputFontMap map[int]int) (result string, err error) {
	if cellXfs.Count > 0 {
		result = fmt.Sprintf(`<cellXfs count="%d">`, cellXfs.Count)
		for _, xf := range cellXfs.Xf {
			var xxf string
			xxf, err = xf.Marshal(outputBorderMap, outputFillMap, outputFontMap)
			if err != nil {
				return
			}
			result += xxf
		}
		result += `</cellXfs>`
	}
	return
}

// xlsxXf directly maps the xf element in the namespace
// http://schemas.openxmlformats.org/spreadsheetml/2006/main -
// currently I have not checked it for completeness - it does as much
// as I need.
type xlsxXf struct {
	ApplyAlignment    bool          `xml:"applyAlignment,attr"`
	ApplyBorder       bool          `xml:"applyBorder,attr"`
	ApplyFont         bool          `xml:"applyFont,attr"`
	ApplyFill         bool          `xml:"applyFill,attr"`
	ApplyNumberFormat bool          `xml:"applyNumberFormat,attr"`
	ApplyProtection   bool          `xml:"applyProtection,attr"`
	BorderId          int           `xml:"borderId,attr"`
	FillId            int           `xml:"fillId,attr"`
	FontId            int           `xml:"fontId,attr"`
	NumFmtId          int           `xml:"numFmtId,attr"`
	Alignment         xlsxAlignment `xml:"alignment"`
}

func (xf *xlsxXf) Equals(other xlsxXf) bool {
	return xf.ApplyAlignment == other.ApplyAlignment &&
		xf.ApplyBorder == other.ApplyBorder &&
		xf.ApplyFont == other.ApplyFont &&
		xf.ApplyFill == other.ApplyFill &&
		xf.ApplyProtection == other.ApplyProtection &&
		xf.BorderId == other.BorderId &&
		xf.FillId == other.FillId &&
		xf.FontId == other.FontId &&
		xf.NumFmtId == other.NumFmtId &&
		xf.Alignment.Equals(other.Alignment)
}

func (xf *xlsxXf) Marshal(outputBorderMap, outputFillMap, outputFontMap map[int]int) (result string, err error) {
	var xAlignment string
	result = fmt.Sprintf(`<xf applyAlignment="%b" applyBorder="%b" applyFont="%b" applyFill="%b" applyNumberFormat="%b" applyProtection="%b" borderId="%d" fillId="%d" fontId="%d" numFmtId="%d">`, bool2Int(xf.ApplyAlignment), bool2Int(xf.ApplyBorder), bool2Int(xf.ApplyFont), bool2Int(xf.ApplyFill), bool2Int(xf.ApplyNumberFormat), bool2Int(xf.ApplyProtection), outputBorderMap[xf.BorderId], outputFillMap[xf.FillId], outputFontMap[xf.FontId], xf.NumFmtId)
	xAlignment, err = xf.Alignment.Marshal()
	if err != nil {
		return
	}
	result += xAlignment
	result += `</xf>`
	return
}

type xlsxAlignment struct {
	Horizontal   string `xml:"horizontal,attr"`
	Indent       int    `xml:"indent,attr"`
	ShrinkToFit  bool   `xml:"shrinkToFit,attr"`
	TextRotation int    `xml:"textRotation,attr"`
	Vertical     string `xml:"vertical,attr"`
	WrapText     bool   `xml:"wrapText,attr"`
}

func (alignment *xlsxAlignment) Equals(other xlsxAlignment) bool {
	return alignment.Horizontal == other.Horizontal &&
		alignment.Indent == other.Indent &&
		alignment.ShrinkToFit == other.ShrinkToFit &&
		alignment.TextRotation == other.TextRotation &&
		alignment.Vertical == other.Vertical &&
		alignment.WrapText == other.WrapText
}

func (alignment *xlsxAlignment) Marshal() (result string, err error) {
	result = fmt.Sprintf(`<alignment horizontal="%s" indent="%d" shrinkToFit="%b" textRotation="%d" vertical="%s" wrapText="%b"/>`, alignment.Horizontal, alignment.Indent, bool2Int(alignment.ShrinkToFit), alignment.TextRotation, alignment.Vertical, bool2Int(alignment.WrapText))
	return
}

func bool2Int(b bool) int {
	if b {
		return 1
	}
	return 0
}
