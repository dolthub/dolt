package textdb

import (
	"bytes"
	"unicode"
)

type Row struct {
	ColVals []string
	RowDesc RowDescription
}

func NewRow(rowDesc RowDescription) *Row {
	return &Row{make([]string, rowDesc.NumCols()), rowDesc}
}

func NewTestRow(colVals ...string) *Row {
	return &Row{colVals, nil}
}

func (r *Row) NumCols() int {
	return r.RowDesc.NumCols()
}

func (r *Row) GetColByIndex(i int) (string, string) {
	return r.RowDesc.GetCol(i), r.ColVals[i]
}

func (r *Row) SetColByIndex(i int, val string) {
	r.ColVals[i] = val
}

func (r *Row) SetColumn(col string, val string) {
	index := r.RowDesc.GetColIndex(col)

	if index == -1 {
		panic("Column " + col + " doesnt exist")
	}

	r.ColVals[index] = val
}

func (r *Row) GetColumn(name string) (string, bool) {
	i := r.RowDesc.GetColIndex(name)

	if i == -1 {
		return "", false
	}

	return r.ColVals[i], true
}

func (r *Row) GetOrDefault(name, def string) string {
	res, ok := r.GetColumn(name)

	if !ok || len(res) == 0 {
		return def
	}

	return res
}

func (r *Row) IsValid() bool {
	for i, val := range r.ColVals {
		if r.RowDesc.IsRequired(i) {
			empty := true
			for _, c := range val {
				if !unicode.IsSpace(c) {
					empty = false
					break
				}
			}

			if empty {
				return false
			}
		}
	}

	return true
}

func (r *Row) Equals(other *Row) bool {
	if len(r.ColVals) != len(other.ColVals) {
		return false
	}

	for i, val := range r.ColVals {
		if val != other.ColVals[i] {
			return false
		}
	}

	return true
}

func (r *Row) String() string {
	var backing [1024]byte
	buf := bytes.NewBuffer(backing[:0])
	buf.WriteString(" {")
	for i, val := range r.ColVals {
		col := r.RowDesc.GetCol(i)
		buf.WriteString(col)
		buf.WriteString(" : ")
		buf.WriteString(val)
		buf.WriteString(" | ")
	}
	buf.WriteString(" }")
	return buf.String()
}
