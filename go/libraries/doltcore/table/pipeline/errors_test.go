package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"testing"
)

func TestTransformRowFailure(t *testing.T) {
	_, sch := untyped.NewUntypedSchema("a", "b", "c")
	r := untyped.NewRowFromStrings(sch, []string{"1", "2", "3"})

	var err error
	err = &TransformRowFailure{r, "transform_name", "details"}

	if !IsTransformFailure(err) {
		t.Error("should be transform failure")
	}

	tn := GetTransFailureTransName(err)
	if tn != "transform_name" {
		t.Error("Unexpected transform name:" + tn)
	}

	fr := GetTransFailureRow(err)

	if !row.AreEqual(types.Format_7_18, r, fr, sch) {
		t.Error("unexpected row")
	}

	dets := GetTransFailureDetails(err)

	if dets != "details" {
		t.Error("unexpected details:" + dets)
	}
}
