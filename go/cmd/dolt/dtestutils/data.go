package dtestutils

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped"
	"strconv"
)

var UUIDS = []uuid.UUID{
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000000")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001")),
	uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000002"))}
var Names = []string{"Bill Billerson", "John Johnson", "Rob Robertson"}
var Ages = []uint64{32, 25, 21}
var Titles = []string{"Senior Dufus", "Dufus", ""}
var MaritalStatus = []bool{true, false, false}

var UntypedSchema = untyped.NewUntypedSchema([]string{"id", "name", "age", "title", "is_married"})
var TypedSchema = schema.NewSchema([]*schema.Field{
	schema.NewField("id", types.UUIDKind, true),
	schema.NewField("name", types.StringKind, true),
	schema.NewField("age", types.UintKind, true),
	schema.NewField("title", types.StringKind, false),
	schema.NewField("is_married", types.BoolKind, true),
})

func init() {
	TypedSchema.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))
	UntypedSchema.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{0}))
}

func CreateTestDataTable(typed bool) (*table.InMemTable, *schema.Schema) {
	sch := TypedSchema
	if !typed {
		sch = UntypedSchema
	}

	imt := table.NewInMemTable(sch)

	for i := 0; i < len(UUIDS); i++ {
		var valsMap map[string]types.Value

		if typed {
			valsMap = map[string]types.Value{
				"id":         types.UUID(UUIDS[i]),
				"name":       types.String(Names[i]),
				"age":        types.Uint(Ages[i]),
				"title":      types.String(Titles[i]),
				"is_married": types.Bool(MaritalStatus[i]),
			}
		} else {
			marriedStr := "true"
			if !MaritalStatus[i] {
				marriedStr = "false"
			}

			valsMap = map[string]types.Value{
				"id":         types.String(UUIDS[i].String()),
				"name":       types.String(Names[i]),
				"age":        types.String(strconv.FormatUint(Ages[i], 10)),
				"title":      types.String(Titles[i]),
				"is_married": types.String(marriedStr),
			}
		}
		imt.AppendRow(table.NewRow(table.RowDataFromValMap(sch, valsMap)))
	}

	return imt, sch
}
