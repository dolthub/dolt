package diff

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

type SchemaChangeType int

const (
	// SchDiffNone is the SchemaChangeType for two columns with the same tag that are identical
	SchDiffNone SchemaChangeType = iota
	// SchDiffAdded is the SchemaChangeType when a column is in the new schema but not the old
	SchDiffColAdded
	// SchDiffRemoved is the SchemaChangeType when a column is in the old schema but not the new
	SchDiffColRemoved
	// SchDiffModified is the SchemaChangeType for two columns with the same tag that are different
	SchDiffColModified
)

// SchemaDifference is the result of comparing two columns from two schemas.
type SchemaDifference struct {
	DiffType SchemaChangeType
	Tag      uint64
	Old      *schema.Column
	New      *schema.Column
}

// DiffSchemas compares two schemas by looking at column's with the same tag.
func DiffSchemas(sch1, sch2 schema.Schema) map[uint64]SchemaDifference {
	colPairMap := pairColumns(sch1, sch2)

	diffs := make(map[uint64]SchemaDifference)
	for tag, colPair := range colPairMap {
		if colPair[0] == nil {
			diffs[tag] = SchemaDifference{SchDiffColAdded, tag, nil, colPair[1]}
		} else if colPair[1] == nil {
			diffs[tag] = SchemaDifference{SchDiffColRemoved, tag, colPair[0], nil}
		} else if !colPair[0].Equals(*colPair[1]) {
			diffs[tag] = SchemaDifference{SchDiffColModified, tag, colPair[0], colPair[1]}
		} else {
			diffs[tag] = SchemaDifference{SchDiffNone, tag, colPair[0], colPair[1]}
		}
	}

	return diffs
}

// pairColumns loops over both sets of columns pairing columns with the same tag.
func pairColumns(sch1, sch2 schema.Schema) map[uint64][2]*schema.Column {
	colPairMap := make(map[uint64][2]*schema.Column)
	sch1.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		colPairMap[tag] = [2]*schema.Column{&col, nil}
		return false
	})

	sch2.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		if pair, ok := colPairMap[tag]; ok {
			pair[1] = &col
			colPairMap[tag] = pair
		} else {
			colPairMap[tag] = [2]*schema.Column{nil, &col}
		}

		return false
	})

	return colPairMap
}
