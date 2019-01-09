package table

import (
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"testing"
)

const (
	pkName = "id"
	pkType = types.UUIDKind
)

var testID, _ = uuid.NewRandom()

func createTestSchema() *schema.Schema {
	fields := []*schema.Field{
		schema.NewField(pkName, pkType, true),
		schema.NewField("first", types.StringKind, true),
		schema.NewField("last", types.StringKind, true),
		schema.NewField("is_married", types.BoolKind, false),
		schema.NewField("age", types.UintKind, false),
		schema.NewField("empty", types.IntKind, false),
	}
	sch := schema.NewSchema(fields)
	err := sch.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))

	if err != nil {
		panic("Error creating schema")
	}

	return sch
}

func newRowTest(sch *schema.Schema) *Row {
	return NewRow(RowDataFromValues(sch, []types.Value{
		types.UUID(testID),
		types.String("bill"),
		types.String("billerson"),
		types.NullValue,
		types.Uint(53),
	}))
}

func newRowFromPKAndValueListTest(sch *schema.Schema) *Row {
	dbSPec, _ := spec.ForDatabase("mem")
	db := dbSPec.GetDatabase()
	pk := types.UUID(testID)
	valList := types.NewList(db, types.String("bill"), types.String("billerson"), types.NullValue, types.Uint(53))
	return NewRow(RowDataFromPKAndValueList(sch, pk, valList))
}

func newRowFromValMapTest(sch *schema.Schema) *Row {
	return NewRow(RowDataFromValMap(sch, map[string]types.Value{
		"id":         types.UUID(testID),
		"first":      types.String("bill"),
		"last":       types.String("billerson"),
		"age":        types.Uint(53),
		"is_married": types.NullValue,
	}))
}

func TestNewRow(t *testing.T) {
	dbSPec, _ := spec.ForDatabase("mem")
	db := dbSPec.GetDatabase()

	tSchema := createTestSchema()
	missingIndex := tSchema.GetFieldIndex("missing")

	if missingIndex != -1 {
		t.Error("Bad index for nonexistant field")
	}

	newRowFuncs := map[string]func(*schema.Schema) *Row{
		"NewRow":                   newRowTest,
		"NewRowFromPKAndValueList": newRowFromPKAndValueListTest,
		"NewRowFromValMap":         newRowFromValMapTest,
	}

	for name, newRowFunc := range newRowFuncs {
		row := newRowFunc(tSchema)
		sch := row.GetSchema()

		if !RowIsValid(row) {
			t.Error(name + " created an invalid row")
		} else if sch.NumFields() != 6 {
			t.Error(name + " created a row with != 5 fields")
		} else if sch.GetPKIndex() == -1 {
			t.Error("Primary key constaint missing.")
		} else {
			rowData := row.CurrData()
			for i := 0; i < sch.NumFields(); i++ {
				val, field := rowData.GetField(i)

				switch field.NameStr() {
				case "id":
					if !val.Equals(types.UUID(testID)) {
						t.Error(name+":", "Value of id is incorrect")
					}

					if !val.Equals(GetPKFromRow(row)) {
						t.Error("Unexpected pk value")
					}

				case "first":
					if !val.Equals(types.String("bill")) {
						t.Error(name+":", "Value of first is incorrect.")
					}
				case "last":
					if !val.Equals(types.String("billerson")) {
						t.Error(name+":", "Value of last is incorrect.")
					}
				case "age":
					if !val.Equals(types.Uint(53)) {
						t.Error(name+":", "Value of age is incorrect.")
					}
				case "empty":
					if val != nil {
						t.Error(name+":", "unexpected val for empty")
					}
				case "is_married":
					if !types.IsNull(val) {
						t.Error(name+":", "unexpected val for is_married")
					}
				default:
					t.Error(name+":", "Unknown field:", field.NameStr())
				}
			}

			row2 := NewRow(RowDataFromPKAndValueList(sch, GetPKFromRow(row), GetNonPKFieldListFromRow(row, db)))

			if !RowsEqualIgnoringSchema(row, row2) {
				t.Error(RowFmt(row), "!=", RowFmt(row2))
			}
		}

		t.Log(RowFmt(row))
	}
}

func createTestSchema2() *schema.Schema {
	fields := []*schema.Field{
		schema.NewField(pkName, pkType, true),
		schema.NewField("first", types.StringKind, true),
		schema.NewField("last", types.StringKind, true),
		schema.NewField("is_married", types.BoolKind, false),
		schema.NewField("age", types.UintKind, false),
	}
	return schema.NewSchema(fields)
}

func TestEqualsIgnoreSchema(t *testing.T) {
	sch := createTestSchema()
	ts2 := createTestSchema2()

	row := newRowFromPKAndValueListTest(sch)
	row2 := newRowFromValMapTest(ts2)

	if !RowsEqualIgnoringSchema(row, row2) {
		t.Error("Rows should be equivalent")
	} else if !RowsEqualIgnoringSchema(row2, row) {
		t.Error("Rows should be equivalent")
	}
}

/*func TestUUIDForColVals(t *testing.T) {
	sch := createTestSchema2()
	inMemTestRF := NewRowFactory(sch)

	r1 := inMemTestRF.NewRow(RowDataFromValues([]types.Value{types.String("Robert"), types.String("Robertson"), types.Bool(true), types.Uint(55)}))
	r2 := inMemTestRF.NewRow(RowDataFromValues([]types.Value{types.String("Roberta"), types.String("Robertson"), types.Bool(true), types.Uint(55)}))
	r3 := inMemTestRF.NewRow(RowDataFromValues([]types.Value{types.String("Robby"), types.String("Robertson"), types.Bool(false), types.Uint(55)}))

	colIndices := []int{1, 2, 3}
	uuid1 := r1.UUIDForColVals(colIndices)
	uuid2 := r2.UUIDForColVals(colIndices)
	uuid3 := r3.UUIDForColVals(colIndices)

	if uuid1 != uuid2 {
		t.Error(uuid1.String(), "!=", uuid2.String())
	}

	if uuid1 == uuid3 {
		t.Error(uuid2.String(), "==", uuid3.String())
	}
}*/

func TestRowPKNonPK(t *testing.T) {
	tests := []struct {
		fields []*schema.Field
		idIdx  int
	}{
		{[]*schema.Field{
			schema.NewField(pkName, pkType, true),
			schema.NewField("first", types.StringKind, true),
		}, 0},

		{[]*schema.Field{
			schema.NewField("first", types.StringKind, true),
			schema.NewField(pkName, pkType, true),
		}, 1},

		{[]*schema.Field{
			schema.NewField(pkName, pkType, true),
			schema.NewField("first", types.StringKind, true),
			schema.NewField("last", types.StringKind, true),
		}, 0},

		{[]*schema.Field{
			schema.NewField("first", types.StringKind, true),
			schema.NewField(pkName, pkType, true),
			schema.NewField("last", types.StringKind, true),
		}, 1},

		{[]*schema.Field{
			schema.NewField("first", types.StringKind, true),
			schema.NewField("last", types.StringKind, true),
			schema.NewField(pkName, pkType, true),
		}, 2},
	}

	for i, test := range tests {
		sch := schema.NewSchema(test.fields)
		sch.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{test.idIdx}))

		id, _ := uuid.NewRandom()
		uuidVal := types.UUID(id)
		first := "Billy"
		last := "Bob"
		valMap := map[string]types.Value{
			"first": types.String(first),
			"last":  types.String(last),
			pkName:  uuidVal,
		}

		row := NewRow(RowDataFromValMap(sch, valMap))

		dbSPec, _ := spec.ForDatabase("mem")
		db := dbSPec.GetDatabase()
		row2 := NewRow(RowDataFromPKAndValueList(sch, GetPKFromRow(row), GetNonPKFieldListFromRow(row, db)))

		if !RowsEqualIgnoringSchema(row, row2) {
			t.Error(i, RowFmt(row), "!=", RowFmt(row2))
		}
	}
}
