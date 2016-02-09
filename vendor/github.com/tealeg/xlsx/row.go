package xlsx

type Row struct {
	Cells    []*Cell
	Hidden   bool
	Sheet    *Sheet
	Height   float64
	isCustom bool
}

func (r *Row) SetHeightCM(ht float64) {
	r.Height = ht * 28.3464567 // Convert CM to postscript points
	r.isCustom = true
}

func (r *Row) AddCell() *Cell {
	cell := NewCell(r)
	r.Cells = append(r.Cells, cell)
	r.Sheet.maybeAddCol(len(r.Cells))
	return cell
}
