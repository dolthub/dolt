package resultset

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

// A table result is a set of rows packaged with their schema.
type TableResult struct {
	Rows   []row.Row
	Schema schema.Schema
}
