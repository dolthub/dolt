package table

import (
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"strings"
)

var ErrInvalidRow = errors.New("invalid row")

type BadRow struct {
	Row     row.Row
	Details []string
}

func NewBadRow(r row.Row, details ...string) *BadRow {
	return &BadRow{r, details}
}

func IsBadRow(err error) bool {
	_, ok := err.(*BadRow)

	return ok
}

func GetBadRowRow(err error) row.Row {
	br, ok := err.(*BadRow)

	if !ok {
		panic("Call IsBadRow prior to trying to get the BadRowRow")
	}

	return br.Row
}

func (br *BadRow) Error() string {
	return strings.Join(br.Details, "\n")
}
