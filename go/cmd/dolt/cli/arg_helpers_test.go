package cli

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"testing"
)

func TestParseKeyValues(t *testing.T) {
	const (
		lnColName = "last"
		fnColName = "first"
		mnColName = "middle"
		lnColTag  = 1
		fnColTag  = 0
		mnColTag  = 2
	)

	testKeyColColl, _ := schema.NewColCollection(
		schema.NewColumn(lnColName, lnColTag, types.StringKind, true),
		schema.NewColumn(fnColName, fnColTag, types.StringKind, true),
		schema.NewColumn(mnColName, mnColTag, types.StringKind, true),
	)

	sch := schema.SchemaFromCols(testKeyColColl)

	singleKeyColColl, _ := schema.NewColCollection(
		schema.NewColumn(lnColName, lnColTag, types.StringKind, true),
	)

	singleKeySch := schema.SchemaFromCols(singleKeyColColl)

	tests := []struct {
		sch          schema.Schema
		args         []string
		expectedKeys []types.Value
		expectErr    bool
	}{
		{
			singleKeySch,
			[]string{"robertson"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForTags(singleKeyColColl.Tags, true),
			},
			false,
		},

		{
			singleKeySch,
			[]string{"last", "robertson"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForTags(singleKeyColColl.Tags, true),
			},
			false,
		},
		{
			singleKeySch,
			[]string{"last"},
			[]types.Value{},
			false,
		},
		{
			singleKeySch,
			[]string{"last", "last"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("last")}.NomsTupleForTags(singleKeyColColl.Tags, true),
			},
			false,
		},
		{
			singleKeySch,
			[]string{"last", "robertson", "johnson"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForTags(singleKeyColColl.Tags, true),
				row.TaggedValues{lnColTag: types.String("johnson")}.NomsTupleForTags(singleKeyColColl.Tags, true),
			},
			false,
		},

		{
			sch,
			[]string{"last"},
			nil,
			false,
		},
		{
			sch,
			[]string{"last", "robertson", "johnson"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForTags(testKeyColColl.Tags, true),
				row.TaggedValues{lnColTag: types.String("johnson")}.NomsTupleForTags(testKeyColColl.Tags, true),
			},
			false,
		},
		{
			sch,
			[]string{"first,last", "robert,robertson", "john,johnson"},
			[]types.Value{
				row.TaggedValues{lnColTag: types.String("robertson"), fnColTag: types.String("robert")}.NomsTupleForTags(testKeyColColl.Tags, true),
				row.TaggedValues{lnColTag: types.String("johnson"), fnColTag: types.String("john")}.NomsTupleForTags(testKeyColColl.Tags, true),
			},
			false,
		},
	}

	for _, test := range tests {
		actual, err := ParseKeyValues(test.sch, test.args)

		if test.expectErr != (err != nil) {
			t.Error(test.args, "produced an unexpected error")
		} else {
			longer := len(actual)
			if len(test.expectedKeys) > longer {
				longer = len(test.expectedKeys)
			}

			for i := 0; i < longer; i++ {
				var currActual types.Value = types.NullValue
				var currExpected types.Value = types.NullValue

				if i < len(test.expectedKeys) {
					currExpected = test.expectedKeys[i]
				}

				if i < len(actual) {
					currActual = actual[i]
				}

				if !currActual.Equals(currExpected) {
					t.Error("actual:", types.EncodedValue(context.Background(), currActual), "!= expected:", types.EncodedValue(context.Background(), currExpected))
				}
			}
		}
	}
}
