// Copyright 2019 Liquidata, Inc.
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

package tabular

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

const (
	nameColName  = "name"
	ageColName   = "age"
	titleColName = "title"
	nameColTag   = 0
	ageColTag    = 1
	titleColTag  = 2
)

type StringBuilderCloser struct {
	strings.Builder
}

func (*StringBuilderCloser) Close() error {
	return nil
}

func TestWriter(t *testing.T) {

	var inCols = []schema.Column{
		{Name: nameColName, Tag: nameColTag, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil},
		{Name: ageColName, Tag: ageColTag, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil},
		{Name: titleColName, Tag: titleColTag, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil},
	}
	colColl, _ := schema.NewColCollection(inCols...)
	rowSch := schema.UnkeyedSchemaFromCols(colColl)

	// Simulate fixed-width string values that the table writer needs to function.
	// First value in each array is the column name
	// Note the unicode character in Jim Halpêrt
	names := []string{
		"name          ",
		"Michael Scott ",
		"Pam Beasley   ",
		"Dwight Schrute",
		"Jim Halpêrt   ",
	}
	ages := []string{
		"age   ",
		"43    ",
		"25    ",
		"29    ",
		"<NULL>",
	}
	titles := []string{
		"title                            ",
		"Regional Manager                 ",
		"Secretary                        ",
		"Assistant to the Regional Manager",
		"<NULL>                           ",
	}

	rows := make([]row.Row, len(ages))
	for i := range ages {
		rows[i] = row.New(types.Format_7_18, rowSch, row.TaggedValues{
			nameColTag:  types.String(names[i]),
			ageColTag:   types.String(ages[i]),
			titleColTag: types.String(titles[i]),
		})
	}

	_, outSch := untyped.NewUntypedSchema(nameColName, ageColName, titleColName)

	t.Run("Test single header row", func(t *testing.T) {
		var stringWr StringBuilderCloser
		tableWr := NewTextTableWriter(&stringWr, outSch)

		var expectedTableString = `
+----------------+--------+-----------------------------------+
| name           | age    | title                             |
+----------------+--------+-----------------------------------+
| Michael Scott  | 43     | Regional Manager                  |
| Pam Beasley    | 25     | Secretary                         |
| Dwight Schrute | 29     | Assistant to the Regional Manager |
| Jim Halpêrt    | <NULL> | <NULL>                            |
+----------------+--------+-----------------------------------+
`
		// strip off the first newline, inserted for nice printing
		expectedTableString = strings.Replace(expectedTableString, "\n", "", 1)

		for _, r := range rows {
			tableWr.WriteRow(context.Background(), r)
		}
		tableWr.Close(context.Background())

		assert.Equal(t, expectedTableString, stringWr.String())
	})

	t.Run("Test multiple header rows", func(t *testing.T) {
		var stringWr StringBuilderCloser
		tableWr := NewTextTableWriterWithNumHeaderRows(&stringWr, outSch, 3)

		var expectedTableString = `
+----------------+--------+-----------------------------------+
| name           | age    | title                             |
| Michael Scott  | 43     | Regional Manager                  |
| Pam Beasley    | 25     | Secretary                         |
+----------------+--------+-----------------------------------+
| Dwight Schrute | 29     | Assistant to the Regional Manager |
| Jim Halpêrt    | <NULL> | <NULL>                            |
+----------------+--------+-----------------------------------+
`
		// strip off the first newline, inserted for nice printing
		expectedTableString = strings.Replace(expectedTableString, "\n", "", 1)

		for _, r := range rows {
			tableWr.WriteRow(context.Background(), r)
		}
		tableWr.Close(context.Background())

		assert.Equal(t, expectedTableString, stringWr.String())
	})

	t.Run("Test no header rows", func(t *testing.T) {
		var stringWr StringBuilderCloser
		tableWr := NewTextTableWriterWithNumHeaderRows(&stringWr, outSch, 0)

		var expectedTableString = `
+----------------+--------+-----------------------------------+
| name           | age    | title                             |
| Michael Scott  | 43     | Regional Manager                  |
| Pam Beasley    | 25     | Secretary                         |
| Dwight Schrute | 29     | Assistant to the Regional Manager |
| Jim Halpêrt    | <NULL> | <NULL>                            |
+----------------+--------+-----------------------------------+
`
		// strip off the first newline, inserted for nice printing
		expectedTableString = strings.Replace(expectedTableString, "\n", "", 1)

		for _, r := range rows {
			tableWr.WriteRow(context.Background(), r)
		}
		tableWr.Close(context.Background())

		assert.Equal(t, expectedTableString, stringWr.String())
	})

	t.Run("Test more header rows than data", func(t *testing.T) {
		var stringWr StringBuilderCloser
		tableWr := NewTextTableWriterWithNumHeaderRows(&stringWr, outSch, 100)

		var expectedTableString = `
+----------------+--------+-----------------------------------+
| name           | age    | title                             |
| Michael Scott  | 43     | Regional Manager                  |
| Pam Beasley    | 25     | Secretary                         |
| Dwight Schrute | 29     | Assistant to the Regional Manager |
| Jim Halpêrt    | <NULL> | <NULL>                            |
+----------------+--------+-----------------------------------+
`
		// strip off the first newline, inserted for nice printing
		expectedTableString = strings.Replace(expectedTableString, "\n", "", 1)

		for _, r := range rows {
			tableWr.WriteRow(context.Background(), r)
		}
		tableWr.Close(context.Background())

		assert.Equal(t, expectedTableString, stringWr.String())
	})
}

// TODO: This doesn't work very well, as the weird formatting attests. There doesn't seem to be an exact way to solve
//  this problem, as discussed here:
//  https://github.com/golang/go/issues/8273
func TestEastAsianLanguages(t *testing.T) {

	var inCols = []schema.Column{
		{Name: nameColName, Tag: nameColTag, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil},
		{Name: ageColName, Tag: ageColTag, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil},
		{Name: titleColName, Tag: titleColTag, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil},
	}
	colColl, _ := schema.NewColCollection(inCols...)
	rowSch := schema.UnkeyedSchemaFromCols(colColl)

	// Simulate fixed-width string values that the table writer needs to function.
	// First value in each array is the column name
	// Note the unicode character in Jim Halpêrt
	names := []string{
		"name          ",
		"Michael Scott ",
		"Pam Beasley   ",
		"Dwight Schrute",
		"Jim Halpêrt   ",
		"つのだ☆HIRO    ",
	}
	ages := []string{
		"age   ",
		"43    ",
		"25    ",
		"29    ",
		"<NULL>",
		"aあいう",
	}
	titles := []string{
		"title                            ",
		"Regional Manager                 ",
		"Secretary                        ",
		"Assistant to the Regional Manager",
		"<NULL>                           ",
		"だ/東京特許許可局局長はよく柿喰う客だ   ",
	}

	rows := make([]row.Row, len(ages))
	for i := range ages {
		rows[i] = row.New(types.Format_7_18, rowSch, row.TaggedValues{
			nameColTag:  types.String(names[i]),
			ageColTag:   types.String(ages[i]),
			titleColTag: types.String(titles[i]),
		})
	}

	_, outSch := untyped.NewUntypedSchema(nameColName, ageColName, titleColName)

	t.Run("Test single header row", func(t *testing.T) {
		var stringWr StringBuilderCloser
		tableWr := NewTextTableWriter(&stringWr, outSch)

		var expectedTableString = `
+----------------+--------+-----------------------------------+
| name           | age    | title                             |
+----------------+--------+-----------------------------------+
| Michael Scott  | 43     | Regional Manager                  |
| Pam Beasley    | 25     | Secretary                         |
| Dwight Schrute | 29     | Assistant to the Regional Manager |
| Jim Halpêrt    | <NULL> | <NULL>                            |
| つのだ☆HIRO     | aあいう | だ/東京特許許可局局長はよく柿喰う客だ    |
+-----------------+---------+------------------------------------------+
`

		// strip off the first newline, inserted for nice printing
		expectedTableString = strings.Replace(expectedTableString, "\n", "", 1)

		for _, r := range rows {
			tableWr.WriteRow(context.Background(), r)
		}
		tableWr.Close(context.Background())

		assert.Equal(t, expectedTableString, stringWr.String())
	})
}
