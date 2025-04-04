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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/fatih/color"
	computeDiff "github.com/kylelemons/godebug/diff"
	"github.com/sergi/go-diff/diffmatchpatch"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
)

// FixedWidthDiffTableWriter wraps a |FixedWidthTableWriter| to provide appropriate coloring and a leading diff type
// column to table rows.
type FixedWidthDiffTableWriter struct {
	tableWriter *FixedWidthTableWriter
}

var _ diff.SqlRowDiffWriter = FixedWidthDiffTableWriter{}

func NewFixedWidthDiffTableWriter(schema sql.Schema, wr io.WriteCloser, numSamples int) *FixedWidthDiffTableWriter {
	// leading diff type column with empty name
	schema = append(sql.Schema{&sql.Column{
		Name: " ",
		Type: types.Text,
	}}, schema...)

	tableWriter := NewFixedWidthTableWriter(schema, wr, numSamples)
	return &FixedWidthDiffTableWriter{
		tableWriter: tableWriter,
	}
}

func (w FixedWidthDiffTableWriter) WriteRow(ctx *sql.Context, row sql.Row, rowDiffType diff.ChangeType, colDiffTypes []diff.ChangeType) error {
	if len(row) != len(colDiffTypes) {
		return fmt.Errorf("expected the same size for columns and diff types, got %d and %d", len(row), len(colDiffTypes))
	}

	diffMarker := ""
	switch rowDiffType {
	case diff.Removed:
		diffMarker = "-"
	case diff.Added:
		diffMarker = "+"
	case diff.ModifiedOld:
		diffMarker = "<"
	case diff.ModifiedNew:
		diffMarker = ">"
	}

	newRow := append(sql.Row{diffMarker}, row...)
	newColDiffTypes := append([]diff.ChangeType{rowDiffType}, colDiffTypes...)

	return w.tableWriter.WriteColoredSqlRow(ctx, newRow, colorsForDiffTypes(newColDiffTypes))
}

func (w FixedWidthDiffTableWriter) WriteCombinedRow(ctx *sql.Context, oldRow, newRow sql.Row, mode diff.Mode) error {
	combinedRow := make([]string, len(oldRow)+1)
	oldRowStrs := make([]string, len(combinedRow))
	newRowStrs := make([]string, len(combinedRow))
	columnDiffs := make([]bool, len(combinedRow))
	widths := make([]FixedWidthString, len(combinedRow))
	hasNewlines := false

	combinedRow[0] = "*"
	oldRowStrs[0] = "<"
	newRowStrs[0] = ">"
	columnDiffs[0] = true
	widths[0] = NewFixedWidthString(combinedRow[0])

	for i := range oldRow {
		var err error
		oldRowStrs[i+1], err = w.tableWriter.stringValue(ctx, i+1, oldRow[i])
		if err != nil {
			return err
		}
		newRowStrs[i+1], err = w.tableWriter.stringValue(ctx, i+1, newRow[i])
		if err != nil {
			return err
		}
		combinedRow[i+1], columnDiffs[i+1], widths[i+1] = w.generateTextDiff(oldRowStrs[i+1], newRowStrs[i+1], mode == diff.ModeInPlace)
		hasNewlines = hasNewlines || (columnDiffs[i+1] && len(widths[i+1].Lines) > 2) || (!columnDiffs[i+1] && len(widths[i+1].Lines) > 1)
	}

	if mode == diff.ModeContext && !hasNewlines {
		oldRowColors := make([]*color.Color, len(combinedRow))
		newRowColors := make([]*color.Color, len(combinedRow))
		for i, hasDiff := range columnDiffs {
			if hasDiff {
				oldRowColors[i] = colorModifiedOld
				newRowColors[i] = colorModifiedNew
			}
		}
		err := w.tableWriter.WriteColoredRow(ctx, oldRowStrs, NewFixedWidthStrings(oldRowStrs), oldRowColors)
		if err != nil {
			return err
		}
		return w.tableWriter.WriteColoredRow(ctx, newRowStrs, NewFixedWidthStrings(newRowStrs), newRowColors)
	}
	return w.tableWriter.WriteColoredRow(ctx, combinedRow, widths, nil)
}

// generateTextDiff returns a new string that represents a diff between the old and new string. The returned string will
// have color applied to it.
func (w FixedWidthDiffTableWriter) generateTextDiff(oldStr string, newStr string, inPlace bool) (result string, hasDiff bool, width FixedWidthString) {
	// The diff routines will modify the strings, and we should just return the original if there will be no diff
	if oldStr == newStr {
		return oldStr, false, NewFixedWidthString(oldStr)
	}

	// coloredStr will be the string that is displayed, which has had color applied to it
	var coloredStr strings.Builder
	// uncoloredStr is the string that is measured to determine display width, as the colors interfere with measuring
	var uncoloredStr strings.Builder
	if inPlace {
		dmp := diffmatchpatch.New()
		diffs := dmp.DiffMain(oldStr, newStr, false)
		for _, diffPart := range diffs {
			uncoloredStr.WriteString(diffPart.Text)
			// We need to end color before any newlines, and reapply it after newlines, else the color will trail to the
			// next line.
			for i, part := range strings.Split(diffPart.Text, "\n") {
				if i > 0 {
					coloredStr.WriteRune('\n')
				}
				switch diffPart.Type {
				case diffmatchpatch.DiffEqual:
					coloredStr.WriteString(part)
				case diffmatchpatch.DiffInsert:
					coloredStr.WriteString(colorModifiedNew.Sprint(part))
				case diffmatchpatch.DiffDelete:
					coloredStr.WriteString(colorModifiedOld.Sprint(part))
				}
			}
		}
	} else {
		diffStrs := strings.Split(computeDiff.Diff(oldStr, newStr), "\n")
		for i, diffStr := range diffStrs {
			if i > 0 {
				uncoloredStr.WriteRune('\n')
				coloredStr.WriteRune('\n')
			}
			if len(diffStr) > 0 {
				uncoloredStr.WriteString(diffStr)
				switch diffStr[0] {
				case '+':
					coloredStr.WriteString(colorModifiedNew.Sprint(diffStr))
				case '-':
					coloredStr.WriteString(colorModifiedOld.Sprint(diffStr))
				default:
					coloredStr.WriteString(diffStr)
				}
			}
		}
	}
	return coloredStr.String(), true, ColoredStringWidth(coloredStr.String(), uncoloredStr.String())
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
