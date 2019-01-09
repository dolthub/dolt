package table

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/pkg/errors"
	"io"
	"sort"
)

type rowStore struct {
	sortFldIdx   int
	sortKeyToRow map[types.Value]*Row
	sortKeys     []types.Value
	isSorted     bool
}

func emptyRowStore(sortFldIdx int) *rowStore {
	return &rowStore{sortFldIdx, make(map[types.Value]*Row), []types.Value{}, false}
}

func rowStoreWithData(sortFldIdx int, rows []*Row) *rowStore {
	sortKeyToRow := make(map[types.Value]*Row, len(rows))
	sortKeys := make([]types.Value, len(rows))

	for i, row := range rows {
		key, _ := row.CurrData().GetField(sortFldIdx)
		sortKeyToRow[key] = row
		sortKeys[i] = key
	}

	rs := &rowStore{sortFldIdx, sortKeyToRow, sortKeys, false}
	rs.sort()

	return rs
}

func (rs *rowStore) addRow(row *Row) {
	key, _ := row.CurrData().GetField(rs.sortFldIdx)
	rs.sortKeyToRow[key] = row
	rs.sortKeys = append(rs.sortKeys, key)
	rs.isSorted = false
}

func (rs *rowStore) sort() {
	sort.Slice(rs.sortKeys, func(i, j int) bool {
		return rs.sortKeys[i].Less(rs.sortKeys[j])
	})
}

// SortingTableReader wraps a TableReader and will allow reading of rows sorted by a particular field ID.  To achieve this
// SortingTableReader will read the entire table into memory before it can be read from.  This approach works ok for smaller
// tables, but will need to do something smarter for larger tables.
type SortingTableReader struct {
	sch        *schema.Schema
	rs         *rowStore
	currentIdx int
}

// NewSortingTableReaderByPK uses the schema of the table being read from and looks at it's primary key constraint to
// determine which field should be used for sorting (If there is no primary key constraint on this schema then this
// function will panic).  Before this function returns all rows will be read from the supplied reader into memory and
// sort them.  The supplied TableReadCloser will be closed when the SortingTableReader is done with it.
func NewSortingTableReaderByPK(rd TableReadCloser, contOnBadRow bool) (*SortingTableReader, int, int, error) {
	pkIdx := rd.GetSchema().GetPKIndex()

	if pkIdx == -1 {
		panic("No Primary Key constraint on the readers schema")
	}
	return NewSortingTableReader(rd, pkIdx, contOnBadRow)
}

// NewSortingTableReader uses a supplied field index to determine which field should be used for sorting. Before this
// function returns all rows will be read from the supplied reader into memory and sort them. The supplied
// TableReadCloser will be closed when the SortingTableReader is done with it.
func NewSortingTableReader(rd TableReadCloser, fldIdx int, contOnBadRow bool) (*SortingTableReader, int, int, error) {
	if fldIdx < 0 || fldIdx >= rd.GetSchema().NumFields() {
		panic("Sorting Field Index is outside the indices of the Schema's fields.")
	}

	rows, numBad, err := ReadAllRows(rd, contOnBadRow)

	if err != nil {
		return nil, len(rows), numBad, err
	}

	rs := rowStoreWithData(fldIdx, rows)

	return &SortingTableReader{rd.GetSchema(), rs, 0}, len(rows), numBad, nil
}

// GetSchema gets the schema of the rows that this reader will return
func (str *SortingTableReader) GetSchema() *schema.Schema {
	return str.sch
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (str *SortingTableReader) ReadRow() (*Row, error) {
	if str.currentIdx == -1 {
		panic("Attempted to read row after close.")
	} else if str.currentIdx >= len(str.rs.sortKeys) {
		return nil, io.EOF
	}

	key := str.rs.sortKeys[str.currentIdx]
	str.currentIdx++

	return str.rs.sortKeyToRow[key], nil
}

// Close should release resources being held.
func (str *SortingTableReader) Close() error {
	if str.currentIdx != -1 {
		str.currentIdx = -1
		str.rs = nil

		return nil
	}

	return errors.New("Already closed.")
}

type SortingTableWriter struct {
	wr        TableWriteCloser
	rs        *rowStore
	contOnErr bool
}

func NewSortingTableWriterByPK(wr TableWriteCloser, contOnErr bool) *SortingTableWriter {
	pkIndex := wr.GetSchema().GetPKIndex()

	if pkIndex == -1 {
		panic("Schema does not have a PK")
	}

	return NewSortingTableWriter(wr, pkIndex, contOnErr)
}

func NewSortingTableWriter(wr TableWriteCloser, sortIdx int, contOnErr bool) *SortingTableWriter {
	return &SortingTableWriter{wr, emptyRowStore(sortIdx), contOnErr}
}

// GetSchema gets the schema of the rows that this writer writes
func (stWr *SortingTableWriter) GetSchema() *schema.Schema {
	return stWr.wr.GetSchema()
}

// WriteRow will write a row to a table
func (stWr *SortingTableWriter) WriteRow(row *Row) error {
	stWr.rs.addRow(row)

	return nil
}

// Close should release resources being held
func (stWr *SortingTableWriter) Close() error {
	if stWr.rs != nil {
		defer stWr.wr.Close()

		stWr.rs.sort()
		for _, key := range stWr.rs.sortKeys {
			row := stWr.rs.sortKeyToRow[key]
			err := stWr.wr.WriteRow(row)

			if err != nil && (!IsBadRow(err) || !stWr.contOnErr) {
				return err
			}
		}

		return nil
	}

	return errors.New("Already closed")
}
