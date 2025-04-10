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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
)

type ConflictVersion string

type FixedWidthConflictTableWriter struct {
	tableWriter *FixedWidthTableWriter
}

// NewFixedWidthConflictTableWriter returns a table writer that prints
// conflicts. |schema| is the schema of the table that the conflicts are being
// printed for.
func NewFixedWidthConflictTableWriter(schema sql.Schema, wr io.WriteCloser, numSamples int) *FixedWidthConflictTableWriter {
	schema = append(sql.Schema{
		// diff type: *, -, +
		&sql.Column{
			Name: " ",
			Type: types.Text,
		},
		// version name: base, ours, theirs
		&sql.Column{
			Name: " ",
			Type: types.Text,
		},
	}, schema...)

	tableWriter := NewFixedWidthTableWriter(schema, wr, numSamples)
	return &FixedWidthConflictTableWriter{
		tableWriter: tableWriter,
	}
}

func (w FixedWidthConflictTableWriter) WriteRow(
	ctx *sql.Context,
	version string,
	row sql.Row,
	rowDiffType diff.ChangeType,
) error {
	diffMarker := ""
	switch rowDiffType {
	case diff.Removed:
		diffMarker = " - "
	case diff.Added:
		diffMarker = " + "
	case diff.ModifiedNew:
		diffMarker = " * "
	}

	newRow := append(sql.Row{diffMarker, version}, row...)
	return w.tableWriter.WriteColoredSqlRow(ctx, newRow, rowColorsForDiffType(rowDiffType, 2, len(row)))
}

func (w FixedWidthConflictTableWriter) Close(ctx context.Context) error {
	return w.tableWriter.Close(ctx)
}

// |n| columns with no colors, |m| columns with a color corresponding to |diffType|.
func rowColorsForDiffType(diffType diff.ChangeType, n int, m int) []*color.Color {
	c := rowConflictColors[diffType]
	colors := make([]*color.Color, n+m)
	for i := 0; i < n+m; i++ {
		if i < n {
			colors[i] = nil
		} else {
			colors[i] = c
		}
	}
	return colors
}

var rowConflictColors = map[diff.ChangeType]*color.Color{
	diff.Added:       color.New(color.FgGreen),
	diff.ModifiedNew: color.New(color.FgYellow),
	diff.Removed:     color.New(color.FgRed, color.CrossedOut),
	diff.None:        nil,
}
