package json

import "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"

type JSONFileInfo struct {
	Rows []row.Row
}

func NewJSONInfo() *JSONFileInfo {
	return &JSONFileInfo{nil}
}

func (info *JSONFileInfo) SetRows(rows []row.Row) *JSONFileInfo {
	info.Rows = rows
	return info
}
