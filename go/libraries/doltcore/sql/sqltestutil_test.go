package sql

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConcatSchemas(t *testing.T) {
	colColl := getTestSchemaUnion(t)

	expectedSch := untyped.UntypeUnkeySchema(schema.UnkeyedSchemaFromCols(colColl))

	// concatSchemas really only makes sense for untyped schemas
	sch := concatSchemas(untypedPeopleSch, untypedEpisodesSch, untypedAppearacesSch)

	assert.Equal(t, expectedSch, sch)
}

func getTestSchemaUnion(t *testing.T) *schema.ColCollection {
	colColl, err := schema.NewColCollection(
		schema.NewColumn("id", 0, types.IntKind, true),
		schema.NewColumn("first", 1, types.StringKind, false),
		schema.NewColumn("last", 2, types.StringKind, false),
		schema.NewColumn("is_married", 3, types.BoolKind, false),
		schema.NewColumn("age", 4, types.IntKind, false),
		schema.NewColumn("rating", 5, types.FloatKind, false),
		schema.NewColumn("uuid", 6, types.UUIDKind, false),
		schema.NewColumn("num_episodes", 7, types.UintKind, false),
		schema.NewColumn("id", 8, types.IntKind, true),
		schema.NewColumn("name", 9, types.StringKind, false),
		schema.NewColumn("air_date", 10, types.IntKind, false),
		schema.NewColumn("rating", 11, types.FloatKind, false),
		schema.NewColumn("character_id", 12, types.IntKind, true),
		schema.NewColumn("episode_id", 13, types.IntKind, true),
		schema.NewColumn("comments", 14, types.StringKind, false),
	)
	if err != nil {
		assert.FailNow(t, "Unexpected error creating schema: %v", err.Error())
	}
	return colColl
}

func TestCombineRows(t *testing.T) {
	sch := concatSchemas(untypedPeopleSch, untypedEpisodesSch, untypedAppearacesSch)

	r := convertRow(t, homer, peopleTestSchema, sch)
	r = combineRows(r, convertRow(t, ep1, episodesTestSchema, sch) , sch)
	r = combineRows(r, convertRow(t, app1, appearancesTestSchema, sch),  sch)

	expectedRow := row.New(sch, row.TaggedValues{
		0:  types.String("0"),
		1:  types.String("Homer"),
		2:  types.String("Simpson"),
		3:  types.String("true"),
		4:  types.String("40"),
		5:  types.String("8.5"),
		6:  types.String(""),
		7:  types.String(""),
		8:  types.String(""),
		9:  types.String("Simpsons Roasting On an Open Fire"),
		10: types.String("629953200"),
		11: types.String("8.0"),
		12: types.String("0"),
		13: types.String("1"),
		14: types.String("Homer is great in this one"),
	})

	assert.Equal(t, expectedRow, r)
}
