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

package fwt

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/store/types"
)

func TestHandleRow(t *testing.T) {
	tests := []struct {
		name         string
		inputRows    []pipeline.RowWithProps
		expectedRows []pipeline.RowWithProps
	}{
		{
			name: "already fixed width",
			inputRows: rs(
				testRow(t, "12345", "12345"),
				testRow(t, "12345", "12345"),
			),
			expectedRows: rs(
				testRow(t, "12345", "12345"),
				testRow(t, "12345", "12345"),
			),
		},
		{
			name: "pad right",
			inputRows: rs(
				testRow(t, "a", "a"),
				testRow(t, "12345", "12345"),
			),
			expectedRows: rs(
				testRow(t, "a    ", "a    "),
				testRow(t, "12345", "12345"),
			),
		},
		// This could be a lot better, but it's exactly as broken as the MySQL shell so we're leaving it as is.
		{
			name: "embedded newlines",
			inputRows: rs(
				testRow(t, "aaaaa\naaaaa", "a"),
				testRow(t, "12345", "12345\n12345"),
			),
			expectedRows: rs(
				testRow(t, "aaaaa\naaaaa", "a          "),
				testRow(t, "12345      ", "12345\n12345"),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := NewAutoSizingFWTTransformer(testSchema(), PrintAllWhenTooLong, 100)
			outChan := make(chan pipeline.RowWithProps)
			badRowChan := make(chan *pipeline.TransformRowFailure)
			stopChan := make(chan struct{})

			go func() {
				for _, r := range tt.inputRows {
					transformer.handleRow(r, outChan, badRowChan, stopChan)
				}
				transformer.flush(outChan, badRowChan, stopChan)
				close(outChan)
			}()

			var outputRows []pipeline.RowWithProps
			for r := range outChan {
				outputRows = append(outputRows, r)
			}

			assert.Equal(t, tt.expectedRows, outputRows)
		})
	}
}

func testSchema() schema.Schema {
	col1 := schema.NewColumn("col1", 0, types.StringKind, false)
	col2 := schema.NewColumn("col2", 1, types.StringKind, false)
	colColl := schema.NewColCollection(col1, col2)
	return schema.UnkeyedSchemaFromCols(colColl)
}

func testRow(t *testing.T, col1, col2 string) pipeline.RowWithProps {
	taggedVals := row.TaggedValues{0: types.String(col1), 1: types.String(col2)}
	r, err := row.New(types.Format_Default, testSchema(), taggedVals)
	assert.NoError(t, err)
	return pipeline.RowWithProps{Row: r, Props: pipeline.NoProps}
}

func rs(rs ...pipeline.RowWithProps) []pipeline.RowWithProps {
	return rs
}
