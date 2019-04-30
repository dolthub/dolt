package row

import (
	"context"
	"testing"
)

func TestFmt(t *testing.T) {
	r := newTestRow()

	expected := `first:"rick" | last:"astley" | age:53 | address:"123 Fake St" | title:null_value | `
	actual := Fmt(context.Background(), r, sch)
	if expected != actual {
		t.Errorf("expected: '%s', actual: '%s'", expected, actual)
	}
}
