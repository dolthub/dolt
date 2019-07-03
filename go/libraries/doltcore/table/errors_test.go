package table

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"testing"
)

func TestBadRow(t *testing.T) {
	cols, _ := schema.NewColCollection(schema.NewColumn("id", 0, types.IntKind, true))
	sch := schema.SchemaFromCols(cols)
	emptyRow := row.New(sch, row.TaggedValues{})

	err := NewBadRow(emptyRow, "details")

	if !IsBadRow(err) {
		t.Error("Should be a bad row error")
	}

	if !row.AreEqual(types.Format_7_18, GetBadRowRow(err), emptyRow, sch) {
		t.Error("did not get back expected empty row")
	}

	if err.Error() != "details" {
		t.Error("unexpected details")
	}
}
