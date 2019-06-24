package xlsx

import "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"

type XLSXFileInfo struct {
	Rows []row.Row
}

func NewXLSXInfo() *XLSXFileInfo {
	return &XLSXFileInfo{nil}
}

func (info *XLSXFileInfo) SetRows(rows []row.Row) *XLSXFileInfo {
	info.Rows = rows
	return info
}
