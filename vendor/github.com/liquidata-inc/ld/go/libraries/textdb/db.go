package textdb

type DB struct {
	rows    []*Row
	RowDesc RowDescription
}

func NewDB(desc RowDescription) *DB {
	return &DB{nil, desc}
}

func NewTestDB(rows ...*Row) *DB {
	return &DB{rows, nil}
}

func (db *DB) CreateRow() *Row {
	return NewRow(db.RowDesc)
}

func (db *DB) AppendRow(row *Row) {
	db.rows = append(db.rows, row)
}

func (db *DB) Equals(other *DB) bool {
	numRows := len(db.rows)

	if numRows != len(other.rows) {
		return false
	}

	for i := 0; i < numRows; i++ {
		if !db.rows[i].Equals(other.rows[i]) {
			return false
		}
	}

	return true
}
