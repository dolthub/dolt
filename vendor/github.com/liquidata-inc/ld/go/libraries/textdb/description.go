package textdb

import (
	"github.com/liquidata-inc/ld/go/libraries/ldset"
)

type RowDescription interface {
	NumCols() int
	GetCol(int) string
	IsRequired(int) bool
	GetColIndex(colName string) int
}

type ColumnInfo struct {
	Name     string
	Required bool
}

type rowDesc struct {
	numCols    int
	required   []bool
	columns    []string
	colIndices map[string]int
}

func RowDescFromColNames(colNames []string, requiredCols []string) RowDescription {
	cols := make([]ColumnInfo, len(colNames))
	setReqCols := ldset.NewSet(requiredCols)

	for i, colName := range colNames {
		cols[i] = ColumnInfo{colName, setReqCols.Contains(colName)}
	}

	return NewRowDesc(cols)
}

func NewRowDesc(colInfo []ColumnInfo) RowDescription {
	cols := make([]string, len(colInfo))
	required := make([]bool, len(colInfo))
	colIndices := make(map[string]int)
	for i, info := range colInfo {
		cols[i] = info.Name
		required[i] = info.Required
		colIndices[info.Name] = i
	}

	if len(cols) != len(colIndices) {
		panic("Column with same name described multiple times")
	}

	return &rowDesc{len(cols), required, cols, colIndices}
}

func (desc *rowDesc) NumCols() int {
	return desc.numCols
}

func (desc *rowDesc) GetCol(index int) string {
	return desc.columns[index]
}

func (desc *rowDesc) IsRequired(index int) bool {
	return desc.required[index]
}

func (desc *rowDesc) GetColIndex(colName string) int {
	if index, ok := desc.colIndices[colName]; ok {
		return index
	}

	return -1
}
