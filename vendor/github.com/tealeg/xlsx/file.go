package xlsx

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// File is a high level structure providing a slice of Sheet structs
// to the user.
type File struct {
	worksheets     map[string]*zip.File
	referenceTable *RefTable
	Date1904       bool
	styles         *xlsxStyleSheet
	Sheets         []*Sheet
	Sheet          map[string]*Sheet
	theme          *theme
}

// Create a new File
func NewFile() (file *File) {
	file = &File{}
	file.Sheet = make(map[string]*Sheet)
	file.Sheets = make([]*Sheet, 0)
	return
}

// OpenFile() take the name of an XLSX file and returns a populated
// xlsx.File struct for it.
func OpenFile(filename string) (file *File, err error) {
	var f *zip.ReadCloser
	f, err = zip.OpenReader(filename)
	if err != nil {
		return nil, err
	}
	file, err = ReadZip(f)
	return
}

// OpenBinary() take bytes of an XLSX file and returns a populated
// xlsx.File struct for it.
func OpenBinary(bs []byte) (file *File, err error) {
	r := bytes.NewReader(bs)
	file, err = OpenReaderAt(r, int64(r.Len()))
	return
}

// OpenReaderAt() take io.ReaderAt of an XLSX file and returns a populated
// xlsx.File struct for it.
func OpenReaderAt(r io.ReaderAt, size int64) (file *File, err error) {
	var f *zip.Reader
	f, err = zip.NewReader(r, size)
	if err != nil {
		return nil, err
	}
	file, err = ReadZipReader(f)
	return
}

// A convenient wrapper around File.ToSlice, FileToSlice will
// return the raw data contained in an Excel XLSX file as three
// dimensional slice.  The first index represents the sheet number,
// the second the row number, and the third the cell number.
//
// For example:
//
//    var mySlice [][][]string
//    var value string
//    mySlice = xlsx.FileToSlice("myXLSX.xlsx")
//    value = mySlice[0][0][0]
//
// Here, value would be set to the raw value of the cell A1 in the
// first sheet in the XLSX file.
func FileToSlice(path string) ([][][]string, error) {
	f, err := OpenFile(path)
	if err != nil {
		return nil, err
	}
	return f.ToSlice()
}

// Save the File to an xlsx file at the provided path.
func (f *File) Save(path string) (err error) {
	var target *os.File

	target, err = os.Create(path)
	if err != nil {
		return
	}

	err = f.Write(target)
	if err != nil {
		return
	}

	return target.Close()
}

// Write the File to io.Writer as xlsx
func (f *File) Write(writer io.Writer) (err error) {
	var parts map[string]string
	var zipWriter *zip.Writer

	parts, err = f.MarshallParts()
	if err != nil {
		return
	}

	zipWriter = zip.NewWriter(writer)

	for partName, part := range parts {
		var writer io.Writer
		writer, err = zipWriter.Create(partName)
		if err != nil {
			return
		}
		_, err = writer.Write([]byte(part))
		if err != nil {
			return
		}
	}

	err = zipWriter.Close()

	return
}

// Add a new Sheet, with the provided name, to a File
func (f *File) AddSheet(sheetName string) (sheet *Sheet, err error) {
	if _, exists := f.Sheet[sheetName]; exists {
		return nil, fmt.Errorf("Duplicate sheet name '%s'.", sheetName)
	}
	sheet = &Sheet{Name: sheetName, File: f}
	if len(f.Sheets) == 0 {
		sheet.Selected = true
	}
	f.Sheet[sheetName] = sheet
	f.Sheets = append(f.Sheets, sheet)
	return sheet, nil
}

func (f *File) makeWorkbook() xlsxWorkbook {
	var workbook xlsxWorkbook
	workbook = xlsxWorkbook{}
	workbook.FileVersion = xlsxFileVersion{}
	workbook.FileVersion.AppName = "Go XLSX"
	workbook.WorkbookPr = xlsxWorkbookPr{
		BackupFile:  false,
		ShowObjects: "all"}
	workbook.BookViews = xlsxBookViews{}
	workbook.BookViews.WorkBookView = make([]xlsxWorkBookView, 1)
	workbook.BookViews.WorkBookView[0] = xlsxWorkBookView{
		ActiveTab:            0,
		FirstSheet:           0,
		ShowHorizontalScroll: true,
		ShowSheetTabs:        true,
		ShowVerticalScroll:   true,
		TabRatio:             204,
		WindowHeight:         8192,
		WindowWidth:          16384,
		XWindow:              "0",
		YWindow:              "0"}
	workbook.Sheets = xlsxSheets{}
	workbook.Sheets.Sheet = make([]xlsxSheet, len(f.Sheets))
	workbook.CalcPr.IterateCount = 100
	workbook.CalcPr.RefMode = "A1"
	workbook.CalcPr.Iterate = false
	workbook.CalcPr.IterateDelta = 0.001
	return workbook
}

// Some tools that read XLSX files have very strict requirements about
// the structure of the input XML.  In particular both Numbers on the Mac
// and SAS dislike inline XML namespace declarations, or namespace
// prefixes that don't match the ones that Excel itself uses.  This is a
// problem because the Go XML library doesn't multiple namespace
// declarations in a single element of a document.  This function is a
// horrible hack to fix that after the XML marshalling is completed.
func replaceRelationshipsNameSpace(workbookMarshal string) string {
	newWorkbook := strings.Replace(workbookMarshal, `xmlns:relationships="http://schemas.openxmlformats.org/officeDocument/2006/relationships" relationships:id`, `r:id`, -1)
	// Dirty hack to fix issues #63 and #91; encoding/xml currently
	// "doesn't allow for additional namespaces to be defined in the
	// root element of the document," as described by @tealeg in the
	// comments for #63.
	oldXmlns := `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">`
	newXmlns := `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">`
	return strings.Replace(newWorkbook, oldXmlns, newXmlns, 1)
}

// Construct a map of file name to XML content representing the file
// in terms of the structure of an XLSX file.
func (f *File) MarshallParts() (map[string]string, error) {
	var parts map[string]string
	var refTable *RefTable = NewSharedStringRefTable()
	refTable.isWrite = true
	var workbookRels WorkBookRels = make(WorkBookRels)
	var err error
	var workbook xlsxWorkbook
	var types xlsxTypes = MakeDefaultContentTypes()

	marshal := func(thing interface{}) (string, error) {
		body, err := xml.Marshal(thing)
		if err != nil {
			return "", err
		}
		return xml.Header + string(body), nil
	}

	parts = make(map[string]string)
	workbook = f.makeWorkbook()
	sheetIndex := 1

	if f.styles == nil {
		f.styles = newXlsxStyleSheet(f.theme)
	}
	f.styles.reset()
	for _, sheet := range f.Sheets {
		xSheet := sheet.makeXLSXSheet(refTable, f.styles)
		rId := fmt.Sprintf("rId%d", sheetIndex)
		sheetId := strconv.Itoa(sheetIndex)
		sheetPath := fmt.Sprintf("worksheets/sheet%d.xml", sheetIndex)
		partName := "xl/" + sheetPath
		types.Overrides = append(
			types.Overrides,
			xlsxOverride{
				PartName:    "/" + partName,
				ContentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"})
		workbookRels[rId] = sheetPath
		workbook.Sheets.Sheet[sheetIndex-1] = xlsxSheet{
			Name:    sheet.Name,
			SheetId: sheetId,
			Id:      rId,
			State:   "visible"}
		parts[partName], err = marshal(xSheet)
		if err != nil {
			return parts, err
		}
		sheetIndex++
	}

	workbookMarshal, err := marshal(workbook)
	if err != nil {
		return parts, err
	}
	workbookMarshal = replaceRelationshipsNameSpace(workbookMarshal)
	parts["xl/workbook.xml"] = workbookMarshal
	if err != nil {
		return parts, err
	}

	parts["_rels/.rels"] = TEMPLATE__RELS_DOT_RELS
	parts["docProps/app.xml"] = TEMPLATE_DOCPROPS_APP
	// TODO - do this properly, modification and revision information
	parts["docProps/core.xml"] = TEMPLATE_DOCPROPS_CORE
	parts["xl/theme/theme1.xml"] = TEMPLATE_XL_THEME_THEME

	xSST := refTable.makeXLSXSST()
	parts["xl/sharedStrings.xml"], err = marshal(xSST)
	if err != nil {
		return parts, err
	}

	xWRel := workbookRels.MakeXLSXWorkbookRels()

	parts["xl/_rels/workbook.xml.rels"], err = marshal(xWRel)
	if err != nil {
		return parts, err
	}

	parts["[Content_Types].xml"], err = marshal(types)
	if err != nil {
		return parts, err
	}

	parts["xl/styles.xml"], err = f.styles.Marshal()
	if err != nil {
		return parts, err
	}

	return parts, nil
}

// Return the raw data contained in the File as three
// dimensional slice.  The first index represents the sheet number,
// the second the row number, and the third the cell number.
//
// For example:
//
//    var mySlice [][][]string
//    var value string
//    mySlice = xlsx.FileToSlice("myXLSX.xlsx")
//    value = mySlice[0][0][0]
//
// Here, value would be set to the raw value of the cell A1 in the
// first sheet in the XLSX file.
func (file *File) ToSlice() (output [][][]string, err error) {
	output = [][][]string{}
	for _, sheet := range file.Sheets {
		s := [][]string{}
		for _, row := range sheet.Rows {
			if row == nil {
				continue
			}
			r := []string{}
			for _, cell := range row.Cells {
				r = append(r, cell.String())
			}
			s = append(s, r)
		}
		output = append(output, s)
	}
	return output, nil
}
