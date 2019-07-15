package row

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

type nomsRow struct {
	key   TaggedValues
	value TaggedValues
	nbf   *types.NomsBinFormat
}

func (nr nomsRow) IterSchema(sch schema.Schema, cb func(tag uint64, val types.Value) (stop bool)) bool {
	stopped := false
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) bool {
		value, _ := nr.GetColVal(tag)
		stopped = cb(tag, value)
		return stopped
	})

	return stopped
}

func (nr nomsRow) IterCols(cb func(tag uint64, val types.Value) (stop bool)) bool {
	stopped := nr.key.Iter(cb)

	if !stopped {
		stopped = nr.value.Iter(cb)
	}

	return stopped
}

func (nr nomsRow) GetColVal(tag uint64) (types.Value, bool) {
	val, ok := nr.key.Get(tag)

	if !ok {
		val, ok = nr.value.Get(tag)
	}

	return val, ok
}

func (nr nomsRow) SetColVal(tag uint64, val types.Value, sch schema.Schema) (Row, error) {
	rowKey := nr.key
	rowVal := nr.value

	cols := sch.GetAllCols()
	col, ok := cols.GetByTag(tag)

	if ok {
		if col.IsPartOfPK {
			rowKey = nr.key.Set(tag, val)
		} else {
			rowVal = nr.value.Set(tag, val)
		}

		return nomsRow{rowKey, rowVal, nr.nbf}, nil
	}

	panic("can't set a column whose tag isn't in the schema.  verify before calling this function.")
}

func (nr nomsRow) Format() *types.NomsBinFormat {
	return nr.nbf
}

func New(nbf *types.NomsBinFormat, sch schema.Schema, colVals TaggedValues) Row {
	allCols := sch.GetAllCols()

	keyVals := make(TaggedValues)
	nonKeyVals := make(TaggedValues)

	colVals.Iter(func(tag uint64, val types.Value) (stop bool) {
		col, ok := allCols.GetByTag(tag)

		if !ok {
			panic("Trying to set a value on an unknown tag is a bug.  Validation should happen upstream.")
		} else if col.IsPartOfPK {
			keyVals[tag] = val
		} else {
			nonKeyVals[tag] = val
		}
		return false
	})

	return fromTaggedVals(nbf, sch, keyVals, nonKeyVals)
}

// fromTaggedVals will take a schema, a map of tag to value for the key, and a map of tag to value for non key values,
// and generates a row.  When a schema adds or removes columns from the non-key portion of the row, the schema will be
// updated, but the rows will not be touched.  So the non-key portion of the row may contain values that are not in the
// schema (The keys must match the schema though).
func fromTaggedVals(nbf *types.NomsBinFormat, sch schema.Schema, keyVals, nonKeyVals TaggedValues) Row {
	allCols := sch.GetAllCols()

	keyVals.Iter(func(tag uint64, val types.Value) (stop bool) {
		col, ok := allCols.GetByTag(tag)

		if !ok {
			panic("Trying to set a value on an unknown tag is a bug for the key.  Validation should happen upstream. col:" + col.Name)
		} else if !col.IsPartOfPK {
			panic("writing columns that are not part of the primary key to pk values. col:" + col.Name)
		} else if !types.IsNull(val) && col.Kind != val.Kind() {
			panic("bug.  Setting a value to an incorrect kind. col: " + col.Name)
		}

		return false
	})

	filteredVals := make(TaggedValues, len(nonKeyVals))
	nonKeyVals.Iter(func(tag uint64, val types.Value) (stop bool) {
		col, ok := allCols.GetByTag(tag)
		if !ok {
			return false
		}

		if col.IsPartOfPK {
			panic("writing columns that are part of the primary key to non-pk values. col:" + col.Name)
		} else if !types.IsNull(val) && col.Kind != val.Kind() {
			panic("bug.  Setting a value to an incorrect kind. col:" + col.Name)
		} else {
			filteredVals[tag] = val
		}

		return false
	})

	return nomsRow{keyVals, filteredVals, nbf}
}

func FromNoms(sch schema.Schema, nomsKey, nomsVal types.Tuple) Row {
	key := ParseTaggedValues(nomsKey)
	val := ParseTaggedValues(nomsVal)

	return fromTaggedVals(nomsKey.Format(), sch, key, val)
}

func (nr nomsRow) NomsMapKey(sch schema.Schema) types.LesserValuable {
	return nr.key.NomsTupleForTags(nr.nbf, sch.GetPKCols().Tags, true)
}

func (nr nomsRow) NomsMapValue(sch schema.Schema) types.Valuable {
	return nr.value.NomsTupleForTags(nr.nbf, sch.GetNonPKCols().SortedTags, false)
}
