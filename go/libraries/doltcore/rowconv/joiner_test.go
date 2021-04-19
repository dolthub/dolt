// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rowconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	firstTag uint64 = iota
	lastTag
	ageTag
	cityTag
)

var peopleCols = schema.NewColCollection(
	schema.NewColumn("last", lastTag, types.StringKind, true),
	schema.NewColumn("first", firstTag, types.StringKind, true),
	schema.NewColumn("age", ageTag, types.IntKind, false),
	schema.NewColumn("city", cityTag, types.StringKind, false),
)

var peopleSch = schema.MustSchemaFromCols(peopleCols)

type toJoinAndExpectedResult struct {
	toJoinVals map[string]row.TaggedValues
	expected   map[string]types.Value
}

func TestJoiner(t *testing.T) {
	tests := []struct {
		name         string
		namedSchemas []NamedSchema
		namers       map[string]ColNamingFunc
		toJoin       []toJoinAndExpectedResult
	}{
		{
			name:         "join diff versions of row",
			namedSchemas: []NamedSchema{{"to", peopleSch}, {"from", peopleSch}},
			namers:       map[string]ColNamingFunc{"to": toNamer, "from": fromNamer},
			toJoin: []toJoinAndExpectedResult{
				{
					toJoinVals: map[string]row.TaggedValues{
						"from": {
							lastTag:  types.String("Richardson"),
							firstTag: types.String("Richard"),
							ageTag:   types.Int(42),
							cityTag:  types.String("San Francisco"),
						},
						"to": {
							lastTag:  types.String("Richardson"),
							firstTag: types.String("Richard"),
							ageTag:   types.Int(43),
							cityTag:  types.String("Los Angeles"),
						},
					},
					expected: map[string]types.Value{
						"from_last":  types.String("Richardson"),
						"from_first": types.String("Richard"),
						"from_city":  types.String("San Francisco"),
						"from_age":   types.Int(42),

						"to_last":  types.String("Richardson"),
						"to_first": types.String("Richard"),
						"to_city":  types.String("Los Angeles"),
						"to_age":   types.Int(43),
					},
				},
				{
					toJoinVals: map[string]row.TaggedValues{
						"from": {
							lastTag:  types.String("Richardson"),
							firstTag: types.String("Richard"),
							ageTag:   types.Int(42),
							cityTag:  types.String("San Francisco"),
						},
					},
					expected: map[string]types.Value{
						"from_last":  types.String("Richardson"),
						"from_first": types.String("Richard"),
						"from_city":  types.String("San Francisco"),
						"from_age":   types.Int(42),
					},
				},
				{
					toJoinVals: map[string]row.TaggedValues{
						"to": {
							lastTag:  types.String("Richardson"),
							firstTag: types.String("Richard"),
							ageTag:   types.Int(43),
							cityTag:  types.String("Los Angeles"),
						},
					},
					expected: map[string]types.Value{
						"to_last":  types.String("Richardson"),
						"to_first": types.String("Richard"),
						"to_city":  types.String("Los Angeles"),
						"to_age":   types.Int(43),
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			j, err := NewJoiner(test.namedSchemas, test.namers)
			require.NoError(t, err)

			for _, tj := range test.toJoin {
				rows := map[string]row.Row{}

				for _, namedSch := range test.namedSchemas {
					r, err := row.New(types.Format_Default, namedSch.Sch, tj.toJoinVals[namedSch.Name])
					require.NoError(t, err)

					rows[namedSch.Name] = r
				}

				joinedRow, err := j.Join(rows)
				require.NoError(t, err)

				joinedSch := j.GetSchema()
				_, err = joinedRow.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
					col, ok := joinedSch.GetAllCols().GetByTag(tag)
					assert.True(t, ok)

					expectedVal := tj.expected[col.Name]
					assert.Equal(t, val, expectedVal)

					return false, nil
				})

				require.NoError(t, err)

				splitRows, err := j.Split(joinedRow)
				require.NoError(t, err)

				assert.Equal(t, len(tj.toJoinVals), len(splitRows))

				for _, namedSch := range test.namedSchemas {
					name := namedSch.Name
					sch := namedSch.Sch
					actual := splitRows[name]
					expectedVals := tj.toJoinVals[name]

					if actual == nil && expectedVals == nil {
						continue
					}

					assert.False(t, actual == nil || expectedVals == nil)

					expected, err := row.New(types.Format_Default, sch, expectedVals)
					require.NoError(t, err)
					assert.True(t, row.AreEqual(actual, expected, sch))
				}
			}
		})
	}
}

func fromNamer(name string) string {
	return "from_" + name
}

func toNamer(name string) string {
	return "to_" + name
}
