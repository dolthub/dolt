package diff

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

const (
	DiffChangeNone types.DiffChangeType = 255
)

type SchemaDifference struct {
	DiffType types.DiffChangeType
	Tag      uint64
	Old      *schema.Column
	New      *schema.Column
}

func DiffSchemas(sch1, sch2 schema.Schema) map[uint64]SchemaDifference {
	colPairMap := pairColumns(sch1, sch2)

	diffs := make(map[uint64]SchemaDifference)
	for tag, colPair := range colPairMap {
		if colPair[0] == nil {
			diffs[tag] = SchemaDifference{types.DiffChangeAdded, tag, nil, colPair[1]}
		} else if colPair[1] == nil {
			diffs[tag] = SchemaDifference{types.DiffChangeRemoved, tag, colPair[0], nil}
		} else if !colPair[0].Equals(*colPair[1]) {
			diffs[tag] = SchemaDifference{types.DiffChangeModified, tag, colPair[0], colPair[1]}
		} else {
			diffs[tag] = SchemaDifference{DiffChangeNone, tag, colPair[0], colPair[1]}
		}
	}

	return diffs
}

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
