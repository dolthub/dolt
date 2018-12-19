package csv

type CSVFileInfo struct {
	Delim         rune
	HasHeaderLine bool
	Columns       []string
	EscapeQuotes  bool
}

func NewCSVInfo() *CSVFileInfo {
	return &CSVFileInfo{',', true, nil, true}
}

func (info *CSVFileInfo) SetDelim(delim rune) *CSVFileInfo {
	info.Delim = delim
	return info
}

func (info *CSVFileInfo) SetHasHeaderLine(hasHeaderLine bool) *CSVFileInfo {
	info.HasHeaderLine = hasHeaderLine
	return info
}

func (info *CSVFileInfo) SetColumns(columns []string) *CSVFileInfo {
	info.Columns = columns
	return info
}

func (info *CSVFileInfo) SetEscapeQuotes(escapeQuotes bool) *CSVFileInfo {
	info.EscapeQuotes = escapeQuotes
	return info
}
