package row

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	lnColName       = "last"
	fnColName       = "first"
	addrColName     = "address"
	ageColName      = "age"
	titleColName    = "title"
	reservedColName = "reserved"
	lnColTag        = 1
	fnColTag        = 0
	addrColTag      = 6
	ageColTag       = 4
	titleColTag     = 40
	reservedColTag  = 50
	unusedTag       = 100
)

var lnVal = types.String("astley")
var fnVal = types.String("rick")
var addrVal = types.String("123 Fake St")
var ageVal = types.Uint(53)
var titleVal = types.NullValue

var testKeyCols = []schema.Column{
	{lnColName, lnColTag, types.StringKind, true, []schema.ColConstraint{schema.NotNullConstraint{}}},
	{fnColName, fnColTag, types.StringKind, true, []schema.ColConstraint{schema.NotNullConstraint{}}},
}
var testCols = []schema.Column{
	{addrColName, addrColTag, types.StringKind, false, nil},
	{ageColName, ageColTag, types.UintKind, false, nil},
	{titleColName, titleColTag, types.StringKind, false, nil},
	{reservedColName, reservedColTag, types.StringKind, false, nil},
}
var testKeyColColl, _ = schema.NewColCollection(testKeyCols...)
var testNonKeyColColl, _ = schema.NewColCollection(testCols...)
var sch, _ = schema.SchemaFromPKAndNonPKCols(testKeyColColl, testNonKeyColColl)

func newTestRow() Row {
	vals := TaggedValues{
		fnColTag:    fnVal,
		lnColTag:    lnVal,
		addrColTag:  addrVal,
		ageColTag:   ageVal,
		titleColTag: titleVal,
	}

	return New(types.Format_7_18, sch, vals)
}

func TestItrRowCols(t *testing.T) {
	r := newTestRow()

	itrVals := make(TaggedValues)
	r.IterCols(func(tag uint64, val types.Value) (stop bool) {
		itrVals[tag] = val
		return false
	})

	assert.Equal(t, TaggedValues{
		lnColTag:    lnVal,
		fnColTag:    fnVal,
		ageColTag:   ageVal,
		addrColTag:  addrVal,
		titleColTag: titleVal,
	}, itrVals)
}

func TestFromNoms(t *testing.T) {
	// New() will faithfully return null values in the row, but such columns won't ever be set when loaded from Noms.
	// So we use a row here with no null values set to avoid this inconsistency.
	expectedRow := New(types.Format_7_18, sch, TaggedValues{
		fnColTag:   fnVal,
		lnColTag:   lnVal,
		addrColTag: addrVal,
		ageColTag:  ageVal,
	})

	t.Run("all values specified", func(t *testing.T) {
		keys := types.NewTuple(types.Format_7_18,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
		)
		vals := types.NewTuple(types.Format_7_18,
			types.Uint(addrColTag), addrVal,
			types.Uint(ageColTag), ageVal,
			types.Uint(titleColTag), titleVal,
		)

		r := FromNoms(sch, keys, vals)
		assert.Equal(t, expectedRow, r)
	})

	t.Run("only key", func(t *testing.T) {
		keys := types.NewTuple(types.Format_7_18,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
		)
		vals := types.NewTuple(types.Format_7_18)

		expectedRow := New(types.Format_7_18, sch, TaggedValues{
			fnColTag: fnVal,
			lnColTag: lnVal,
		})
		r := FromNoms(sch, keys, vals)
		assert.Equal(t, expectedRow, r)
	})

	t.Run("additional tag not in schema is silently dropped", func(t *testing.T) {
		keys := types.NewTuple(types.Format_7_18,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
		)
		vals := types.NewTuple(types.Format_7_18,
			types.Uint(addrColTag), addrVal,
			types.Uint(ageColTag), ageVal,
			types.Uint(titleColTag), titleVal,
			types.Uint(unusedTag), fnVal,
		)

		r := FromNoms(sch, keys, vals)
		assert.Equal(t, expectedRow, r)
	})

	t.Run("bad type", func(t *testing.T) {
		keys := types.NewTuple(types.Format_7_18,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
		)
		vals := types.NewTuple(types.Format_7_18,
			types.Uint(addrColTag), addrVal,
			types.Uint(ageColTag), fnVal,
		)

		assert.Panics(t, func() {
			FromNoms(sch, keys, vals)
		})
	})

	t.Run("key col set in vals", func(t *testing.T) {
		keys := types.NewTuple(types.Format_7_18,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
		)
		vals := types.NewTuple(types.Format_7_18,
			types.Uint(addrColTag), addrVal,
			types.Uint(fnColTag), fnVal,
		)

		assert.Panics(t, func() {
			FromNoms(sch, keys, vals)
		})
	})

	t.Run("unknown tag in key", func(t *testing.T) {
		keys := types.NewTuple(types.Format_7_18,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
			types.Uint(unusedTag), fnVal,
		)
		vals := types.NewTuple(types.Format_7_18,
			types.Uint(addrColTag), addrVal,
			types.Uint(ageColTag), ageVal,
			types.Uint(titleColTag), titleVal,
		)

		assert.Panics(t, func() {
			FromNoms(sch, keys, vals)
		})
	})

	t.Run("value tag in key", func(t *testing.T) {
		keys := types.NewTuple(types.Format_7_18,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
			types.Uint(ageColTag), ageVal,
		)
		vals := types.NewTuple(types.Format_7_18,
			types.Uint(addrColTag), addrVal,
			types.Uint(titleColTag), titleVal,
		)

		assert.Panics(t, func() {
			FromNoms(sch, keys, vals)
		})
	})
}

func TestSetColVal(t *testing.T) {
	t.Run("valid update", func(t *testing.T) {
		expected := map[uint64]types.Value{
			lnColTag:    lnVal,
			fnColTag:    fnVal,
			ageColTag:   ageVal,
			addrColTag:  addrVal,
			titleColTag: titleVal}

		updatedVal := types.String("sanchez")

		r := newTestRow()
		assert.Equal(t, r, New(types.Format_7_18, sch, expected))

		updated, err := r.SetColVal(lnColTag, updatedVal, sch)
		assert.NoError(t, err)

		// validate calling set does not mutate the original row
		assert.Equal(t, r, New(types.Format_7_18, sch, expected))
		expected[lnColTag] = updatedVal
		assert.Equal(t, updated, New(types.Format_7_18, sch, expected))

		// set to a nil value
		updated, err = updated.SetColVal(titleColTag, nil, sch)
		assert.NoError(t, err)
		delete(expected, titleColTag)
		assert.Equal(t, updated, New(types.Format_7_18, sch, expected))
	})

	t.Run("invalid update", func(t *testing.T) {
		expected := map[uint64]types.Value{
			lnColTag:    lnVal,
			fnColTag:    fnVal,
			ageColTag:   ageVal,
			addrColTag:  addrVal,
			titleColTag: titleVal}

		r := newTestRow()

		assert.Equal(t, r, New(types.Format_7_18, sch, expected))

		// SetColVal allows an incorrect type to be set for a column
		updatedRow, err := r.SetColVal(lnColTag, types.Bool(true), sch)
		assert.NoError(t, err)
		// IsValid fails for the type problem
		assert.False(t, IsValid(updatedRow, sch))
		invalidCol := GetInvalidCol(updatedRow, sch)
		assert.NotNil(t, invalidCol)
		assert.Equal(t, uint64(lnColTag), invalidCol.Tag)

		// validate calling set does not mutate the original row
		assert.Equal(t, r, New(types.Format_7_18, sch, expected))
	})
}

func TestConvToAndFromTuple(t *testing.T) {
	ctx := context.Background()

	r := newTestRow()

	keyTpl := r.NomsMapKey(sch).(TupleVals)
	valTpl := r.NomsMapValue(sch).(TupleVals)

	r2 := FromNoms(sch, keyTpl.Value(ctx).(types.Tuple), valTpl.Value(ctx).(types.Tuple))

	fmt.Println(Fmt(context.Background(), r, sch))
	fmt.Println(Fmt(context.Background(), r2, sch))

	if !AreEqual(r, r2, sch) {
		t.Error("Failed to convert to a noms tuple, and then convert back to the same row")
	}
}
