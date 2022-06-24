// Copyright 2022 Dolthub, Inc.
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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
)

// FixedWidthDiffTableWriter wraps a |FixedWidthTableWriter| to provide appropriate coloring and a leading diff type
// column to table rows.
type FixedWidthDiffTableWriter struct {
	tableWriter *FixedWidthTableWriter
}

func NewFixedWidthDiffTableWriter(schema sql.Schema, wr io.WriteCloser, numSamples int) *FixedWidthDiffTableWriter {
	// leading diff type column with empty name
	schema = append(sql.Schema{&sql.Column{
		Name: " ",
		Type: sql.Text,
	}}, schema...)

	tableWriter := NewFixedWidthTableWriter(schema, wr, numSamples)
	return &FixedWidthDiffTableWriter{
		tableWriter: tableWriter,
	}
}

func (w FixedWidthDiffTableWriter) WriteRow(
	ctx context.Context,
	row sql.Row,
	rowDiffType diff.ChangeType,
	colDiffTypes []diff.ChangeType,
) error {
	if len(row) != len(colDiffTypes) {
		return fmt.Errorf("expected the same size for columns and diff types, got %d and %d", len(row), len(colDiffTypes))
	}

	diffMarker := ""
	switch rowDiffType {
	case diff.Deleted:
		diffMarker = "-"
	case diff.Inserted:
		diffMarker = "+"
	case diff.ModifiedOld:
		diffMarker = "<"
	case diff.ModifiedNew:
		diffMarker = ">"
	}

	newRow := append(sql.Row{diffMarker}, row...)
	newColDiffTypes := append([]diff.ChangeType{rowDiffType}, colDiffTypes...)

	return w.tableWriter.WriteRow(ctx, newRow, colorsForDiffTypes(newColDiffTypes))
}

func colorsForDiffTypes(colDiffTypes []diff.ChangeType) []*color.Color {
	colors := make([]*color.Color, len(colDiffTypes))
	for i := range colDiffTypes {
		if colDiffTypes[i] != diff.None {
			colors[i] = colDiffColors[colDiffTypes[i]]
		}
	}

	return colors
}

func (w FixedWidthDiffTableWriter) Close(ctx context.Context) error {
	return w.tableWriter.Close(ctx)
}
