package fwt

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
)

func TestHandleRow(t *testing.T) {
	tests := []struct {
		name   string
		inputRows []pipeline.RowWithProps
		expectedRows []pipeline.RowWithProps
	}{
		{
			name: "already fixed width",
			inputRows: rs(
				testRow("12345", "12345"),
				testRow("12345", "12345"),
			),
			expectedRows: rs(
				testRow("12345", "12345"),
				testRow("12345", "12345"),
			),
		},
		{
			name: "pad right",
			inputRows: rs(
				testRow("a", "a"),
				testRow("12345", "12345"),
			),
			expectedRows: rs(
				testRow("a    ", "a    "),
				testRow("12345", "12345"),
			),
		},
		// This could be a lot better, but it's exactly as broken as the MySQL shell so we're leaving it as is.
		{
			name: "embedded newlines",
			inputRows: rs(
				testRow("aaaaa\naaaaa", "a"),
				testRow("12345", "12345\n12345"),
			),
			expectedRows: rs(
				testRow("aaaaa\naaaaa", "a          "),
				testRow("12345      ", "12345\n12345"),
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
	colColl, _ := schema.NewColCollection(col1, col2)
	return schema.UnkeyedSchemaFromCols(colColl)
}

func testRow(col1, col2 string) pipeline.RowWithProps {
	taggedVals := row.TaggedValues{0: types.String(col1), 1: types.String(col2)}
	return pipeline.RowWithProps{row.New(testSchema(), taggedVals), pipeline.NoProps}
}

func rs(rs ...pipeline.RowWithProps) []pipeline.RowWithProps {
	return rs
}