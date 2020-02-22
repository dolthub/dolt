package table

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"io"
)

type CompositeTableReader struct {
	sch     schema.Schema
	readers []TableReadCloser
	idx     int
}

func NewCompositeTableReader(readers []TableReadCloser) (*CompositeTableReader, error) {
	if len(readers) == 0 {
		panic("nothing to iterate")
	}

	sch := readers[0].GetSchema()
	for i := 1; i < len(readers); i++ {
		otherSch := readers[i].GetSchema()
		eq, err := schema.SchemasAreEqual(sch, otherSch)

		if err != nil {
			return nil, err
		} else if !eq {
			panic("readers must have the same schema")
		}
	}

	return &CompositeTableReader{sch: sch, readers: readers, idx: 0}, nil
}

// GetSchema gets the schema of the rows that this reader will return
func (rd *CompositeTableReader) GetSchema() schema.Schema {
	return rd.sch
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and calling
// IsBadRow(err) will be return true. This is a potentially non-fatal error and callers can decide if they want to
// continue on a bad row, or fail.
func (rd *CompositeTableReader) ReadRow(ctx context.Context) (row.Row, error) {
	for rd.idx < len(rd.readers) {
		r, err := rd.readers[rd.idx].ReadRow(ctx)

		if err == io.EOF {
			rd.idx++
			continue
		} else if err != nil {
			return nil, err
		}

		return r, nil
	}

	return nil, io.EOF
}

// VerifySchema checks that the incoming schema matches the schema from the existing table
func (rd *CompositeTableReader) VerifySchema(outSch schema.Schema) (bool, error) {
	return schema.VerifyInSchema(rd.sch, outSch)
}

// Close should release resources being held
func (rd *CompositeTableReader) Close(ctx context.Context) error {
	var firstErr error
	for _, rdr := range rd.readers {
		err := rdr.Close(ctx)

		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
