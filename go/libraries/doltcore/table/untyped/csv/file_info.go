package csv

// CSVFileInfo describes a csv file
type CSVFileInfo struct {
	// Delim says which character is used as a field delimiter
	Delim string
	// HasHeaderLine says if the csv has a header line which contains the names of the columns
	HasHeaderLine bool
	// Columns can be provided if you no the columns and their order in the csv
	Columns []string
	// EscapeQuotes says whether quotes should be escaped when parsing the csv
	EscapeQuotes bool
}

// NewCSVInfo creates a new CSVInfo struct with default values
func NewCSVInfo() *CSVFileInfo {
	return &CSVFileInfo{",", true, nil, true}
}

// SetDelim sets the Delim member and returns the CSVFileInfo
func (info *CSVFileInfo) SetDelim(delim string) *CSVFileInfo {
	info.Delim = delim
	return info
}

// SetHasHeaderLine sets the HeaderLine member and returns the CSVFileInfo
func (info *CSVFileInfo) SetHasHeaderLine(hasHeaderLine bool) *CSVFileInfo {
	info.HasHeaderLine = hasHeaderLine
	return info
}

// SetColumns sets the Columns member and returns the CSVFileInfo
func (info *CSVFileInfo) SetColumns(columns []string) *CSVFileInfo {
	info.Columns = columns
	return info
}

// SetEscapeQuotes sets the EscapeQuotes member and returns the CSVFileInfo
func (info *CSVFileInfo) SetEscapeQuotes(escapeQuotes bool) *CSVFileInfo {
	info.EscapeQuotes = escapeQuotes
	return info
}
