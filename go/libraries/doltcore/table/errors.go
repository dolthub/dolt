package table

import (
	"strings"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
)

// BadRow is an error which contains the row and details about what is wrong with it.
type BadRow struct {
	Row     row.Row
	Details []string
}

// NewBadRow creates a BadRow instance with a given row and error details
func NewBadRow(r row.Row, details ...string) *BadRow {
	return &BadRow{r, details}
}

// IsBadRow takes an error and returns whether it is a BadRow
func IsBadRow(err error) bool {
	_, ok := err.(*BadRow)

	return ok
}

// GetBadRow will retrieve the Row from the BadRow error
func GetBadRowRow(err error) row.Row {
	br, ok := err.(*BadRow)

	if !ok {
		panic("Call IsBadRow prior to trying to get the BadRowRow")
	}

	return br.Row
}

// Error returns a string with error details.
func (br *BadRow) Error() string {
	return strings.Join(br.Details, "\n")
}
