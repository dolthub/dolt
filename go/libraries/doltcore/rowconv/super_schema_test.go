package rowconv

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/envtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var idColTag0TypeUUID = schema.NewColumn("id", 0, types.UUIDKind, true)
var idColTag0TypeUint = schema.NewColumn("id", 0, types.UintKind, true)
var firstColTag1TypeStr = schema.NewColumn("first", 1, types.StringKind, false)
var lastColTag2TypeStr = schema.NewColumn("last", 2, types.StringKind, false)
var addrColTag3TypeStr = schema.NewColumn("addr", 3, types.StringKind, false)
var titleColTag3TypeStr = schema.NewColumn("title", 3, types.StringKind, false)
var ageColTag3TypeInt = schema.NewColumn("age", 3, types.IntKind, false)
var ageColTag4TypeInt = schema.NewColumn("age", 4, types.IntKind, false)
var ageColTag4TypeUint = schema.NewColumn("age", 4, types.UintKind, false)

func mustMap(m types.Map, err error) types.Map {
	if err != nil {
		panic(err)
	}

	return m
}

func mustSchema(cols ...schema.Column) schema.Schema {
	hasPKCols := false
	for _, col := range cols {
		if col.IsPartOfPK {
			hasPKCols = true
			break
		}
	}

	colColl, err := schema.NewColCollection(cols...)

	if err != nil {
		panic(err)
	}

	if !hasPKCols {
		return schema.UnkeyedSchemaFromCols(colColl)
	} else {
		return schema.SchemaFromCols(colColl)
	}
}

func mustSchemaFromTagAndKind(tts map[string]TagKindPair) schema.Schema {
	cols := make([]schema.Column, 0, len(tts))
	for name, tt := range tts {
		col := schema.NewColumn(name, tt.Tag, tt.Kind, false)
		cols = append(cols, col)
	}

	colColl, err := schema.NewColCollection(cols...)

	if err != nil {
		panic(err)
	}

	return schema.UnkeyedSchemaFromCols(colColl)
}

func TestSuperSchemaGen(t *testing.T) {
	tests := []struct {
		name     string
		schemas  []schema.Schema
		expected schema.Schema
	}{
		{
			"base schema",
			[]schema.Schema{
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr),
			},
			mustSchemaFromTagAndKind(map[string]TagKindPair{
				"0_UUID":   TagKindPair{0, types.UUIDKind},
				"1_String": TagKindPair{1, types.StringKind},
				"2_String": TagKindPair{2, types.StringKind},
			}),
		},
		{
			"differing keys",
			[]schema.Schema{
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr),
				mustSchema(idColTag0TypeUint, firstColTag1TypeStr, lastColTag2TypeStr),
			},
			mustSchemaFromTagAndKind(map[string]TagKindPair{
				"0_UUID":   TagKindPair{0, types.UUIDKind},
				"0_Uint":   TagKindPair{schema.ReservedTagMin, types.UintKind},
				"1_String": TagKindPair{1, types.StringKind},
				"2_String": TagKindPair{2, types.StringKind},
			}),
		},
		{
			"tag conflict",
			[]schema.Schema{
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr),
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr),
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, ageColTag3TypeInt),
			},
			mustSchemaFromTagAndKind(map[string]TagKindPair{
				"0_UUID":   TagKindPair{0, types.UUIDKind},
				"1_String": TagKindPair{1, types.StringKind},
				"2_String": TagKindPair{2, types.StringKind},
				"3_String": TagKindPair{3, types.StringKind},
				"3_Int":    TagKindPair{schema.ReservedTagMin, types.IntKind},
			}),
		},
		{
			"tag type conflict",
			[]schema.Schema{
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr),
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr),
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, titleColTag3TypeStr),
			},
			mustSchemaFromTagAndKind(map[string]TagKindPair{
				"0_UUID":   TagKindPair{0, types.UUIDKind},
				"1_String": TagKindPair{1, types.StringKind},
				"2_String": TagKindPair{2, types.StringKind},
				"3_String": TagKindPair{3, types.StringKind},
			}),
		},
		{
			"multiple tag conflicts",
			[]schema.Schema{
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr),
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr),
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, ageColTag3TypeInt),
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr, ageColTag4TypeInt),
				mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr, ageColTag4TypeUint),
			},
			mustSchemaFromTagAndKind(map[string]TagKindPair{
				"0_UUID":   TagKindPair{0, types.UUIDKind},
				"1_String": TagKindPair{1, types.StringKind},
				"2_String": TagKindPair{2, types.StringKind},
				"3_String": TagKindPair{3, types.StringKind},
				"3_Int":    TagKindPair{schema.ReservedTagMin, types.IntKind},
				"4_Int":    TagKindPair{4, types.IntKind},
				"4_Uint":   TagKindPair{schema.ReservedTagMin + 1, types.UintKind},
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ssg := NewSuperSchemaGen()

			for _, sch := range test.schemas {
				err := ssg.AddSchema(sch)
				require.NoError(t, err)
			}

			result, err := ssg.GenerateSuperSchema()
			require.NoError(t, err)

			eq, err := schema.SchemasAreEqual(result, test.expected)
			require.NoError(t, err)
			assert.True(t, eq)
		})
	}
}

func TestSuperSchemaFromHistory(t *testing.T) {
	const tblName = "test_table"
	initialSch := mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr)
	addAddrAt3Sch := mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr)
	addAgeAt3Sch := mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, ageColTag3TypeInt)
	readdAgeAt4Sch := mustSchema(idColTag0TypeUUID, firstColTag1TypeStr, lastColTag2TypeStr, addrColTag3TypeStr, ageColTag4TypeUint)

	ctx := context.Background()
	dEnv := envtestutils.CreateInitializedTestEnv(t, ctx)

	history := envtestutils.HistoryNode{
		Branch:    "master",
		CommitMsg: "Seeding with initial user data",
		Updates: map[string]envtestutils.TableUpdate{
			tblName: {
				NewSch: initialSch,
			},
		},
		Children: []envtestutils.HistoryNode{
			{
				Branch:    "add-age",
				CommitMsg: "Adding int age to users with tag 3",
				Updates: map[string]envtestutils.TableUpdate{
					tblName: {
						NewSch: addAgeAt3Sch,
					},
				},
				Children: nil,
			},
			{
				Branch:    "master",
				CommitMsg: "Adding string address to users with tag 3",
				Updates: map[string]envtestutils.TableUpdate{
					tblName: {
						NewSch: addAddrAt3Sch,
					},
				},
				Children: []envtestutils.HistoryNode{
					{
						Branch:    "master",
						CommitMsg: "Re-add age as a uint with tag 4",
						Updates: map[string]envtestutils.TableUpdate{
							tblName: {
								NewSch: readdAgeAt4Sch,
							},
						},
						Children: nil,
					},
				},
			},
		},
	}

	envtestutils.InitializeWithHistory(t, ctx, dEnv, history)

	ssg := NewSuperSchemaGen()
	err := ssg.AddHistoryOfTable(ctx, tblName, dEnv.DoltDB)
	require.NoError(t, err)

	result, err := ssg.GenerateSuperSchema(NameKindPair{"extra", types.StringKind})
	require.NoError(t, err)

	expected := mustSchemaFromTagAndKind(map[string]TagKindPair{
		"0_UUID":   TagKindPair{0, types.UUIDKind},
		"1_String": TagKindPair{1, types.StringKind},
		"2_String": TagKindPair{2, types.StringKind},
		"3_Int":    TagKindPair{3, types.IntKind},
		"3_String": TagKindPair{schema.ReservedTagMin, types.StringKind},
		"4_Uint":   TagKindPair{4, types.UintKind},
		"extra":    {schema.ReservedTagMin + 1, types.StringKind},
	})

	eq, err := schema.SchemasAreEqual(result, expected)
	require.NoError(t, err)
	assert.True(t, eq)
}
