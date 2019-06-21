package diff

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"reflect"
	"testing"
)

func TestDiffSchemas(t *testing.T) {
	oldCols := []schema.Column{
		schema.NewColumn("unchanged", 0, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("removed", 1, types.StringKind, true),
		schema.NewColumn("renamed", 2, types.StringKind, false),
		schema.NewColumn("type_changed", 3, types.StringKind, false),
		schema.NewColumn("moved_to_pk", 4, types.StringKind, false),
		schema.NewColumn("contraint_added", 5, types.StringKind, false),
	}

	newCols := []schema.Column{
		schema.NewColumn("unchanged", 0, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("renamed_new", 2, types.StringKind, false),
		schema.NewColumn("type_changed", 3, types.IntKind, false),
		schema.NewColumn("moved_to_pk", 4, types.StringKind, true),
		schema.NewColumn("contraint_added", 5, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("added", 6, types.StringKind, false),
	}

	oldColColl, _ := schema.NewColCollection(oldCols...)
	newColColl, _ := schema.NewColCollection(newCols...)

	oldSch := schema.SchemaFromCols(oldColColl)
	newSch := schema.SchemaFromCols(newColColl)
	diffs := DiffSchemas(oldSch, newSch)

	expected := map[uint64]SchemaDifference{
		0: {SchDiffNone, 0, &oldCols[0], &newCols[0]},
		1: {SchDiffColRemoved, 1, &oldCols[1], nil},
		2: {SchDiffColModified, 2, &oldCols[2], &newCols[1]},
		3: {SchDiffColModified, 3, &oldCols[3], &newCols[2]},
		4: {SchDiffColModified, 4, &oldCols[4], &newCols[3]},
		5: {SchDiffColModified, 5, &oldCols[5], &newCols[4]},
		6: {SchDiffColAdded, 6, nil, &newCols[5]},
	}

	if !reflect.DeepEqual(diffs, expected) {
		t.Error(diffs, "!=", expected)
	}
}
