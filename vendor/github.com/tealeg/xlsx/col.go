package xlsx

// Default column width in excel
const ColWidth = 9.5

type Col struct {
	Min       int
	Max       int
	Hidden    bool
	Width     float64
	Collapsed bool
	numFmt    string
	style     *Style
}

func (c *Col) SetType(cellType CellType) {
	switch cellType {
	case CellTypeString:
		c.numFmt = builtInNumFmt[builtInNumFmtIndex_STRING]
	case CellTypeBool:
		c.numFmt = builtInNumFmt[builtInNumFmtIndex_GENERAL] //TEMP
	case CellTypeNumeric:
		c.numFmt = builtInNumFmt[builtInNumFmtIndex_INT]
	case CellTypeDate:
		c.numFmt = builtInNumFmt[builtInNumFmtIndex_DATE]
	case CellTypeFormula:
		c.numFmt = builtInNumFmt[builtInNumFmtIndex_GENERAL]
	case CellTypeError:
		c.numFmt = builtInNumFmt[builtInNumFmtIndex_GENERAL] //TEMP
	case CellTypeGeneral:
		c.numFmt = builtInNumFmt[builtInNumFmtIndex_GENERAL]
	}
}

// GetStyle returns the Style associated with a Col
func (c *Col) GetStyle() *Style {
	return c.style
}

// SetStyle sets the style of a Col
func (c *Col) SetStyle(style *Style) {
	c.style = style
}
