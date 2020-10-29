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

package fwt

import (
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrRowCountMismatch = errors.New("number of columns passed to formatter does not match expected count")
var ErrColumnTooLong = errors.New("column width exceeded maximum width for column and TooLongBehavior is ErrorWhenTooLong")

type FixedWidthFormatter struct {
	colCount   int
	widths     []int
	maxRunes   []int
	noFitStrs  []string
	totalWidth int

	runeBuff  [][]rune
	tooLngBhv TooLongBehavior
}

func FixedWidthFormatterForSchema(sch schema.Schema, tagToPrintWidth map[uint64]int, tagToMaxRunes map[uint64]int) FixedWidthFormatter {
	allCols := sch.GetAllCols()

	if len(tagToPrintWidth) != allCols.Size() {
		panic("Invalid tagToPrintWidth map should have a value for every field.")
	}

	if len(tagToMaxRunes) != allCols.Size() {
		panic("Invalid tagToMaxRunes map should have a value for every field.")
	}

	allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Kind != types.StringKind {
			panic("Invalid schema argument.  Has non-String fields. Use a rowconverter, or mapping reader / writer")
		}

		return false, nil
	})

	for _, width := range tagToPrintWidth {
		if width > 0 {
		}
	}

	totalWidth := 0
	noFitStrs := make([]string, 0, allCols.Size())
	widths := make([]int, 0, allCols.Size())
	maxRunes := make([]int, 0, allCols.Size())
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		chars := make([]byte, tagToPrintWidth[tag])
		for j := 0; j < tagToPrintWidth[tag]; j++ {
			chars[j] = '#'
		}

		width := tagToPrintWidth[tag]
		if width < 0 {
			width = 0
		}

		totalWidth += width
		noFitStrs = append(noFitStrs, string(chars))
		widths = append(widths, width)
		maxRunes = append(maxRunes, tagToMaxRunes[tag])
		return false, nil
	})

	return FixedWidthFormatter{
		colCount:   len(widths),
		widths:     widths,
		maxRunes:   maxRunes,
		noFitStrs:  noFitStrs,
		totalWidth: totalWidth,
		runeBuff:   nil,
		tooLngBhv:  0,
	}
}

func (fwf FixedWidthFormatter) FormatRow(r row.Row, sch schema.Schema) (row.Row, error) {
	destFields := make(row.TaggedValues)
	idx := 0
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		defer func() {
			idx += 1
		}()

		v, ok := r.GetColVal(tag)

		var formattedStr string
		if ok {
			formattedStr, err = fwf.FormatColumn(string(v.(types.String)), idx)

			if err != nil {
				return
			}
		}

		destFields[tag] = types.String(formattedStr)

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return row.New(r.Format(), sch, destFields)
}

func (fwf FixedWidthFormatter) Format(cols []string) ([]string, error) {
	if len(cols) != fwf.colCount {
		return nil, ErrRowCountMismatch
	}

	formatted := make([]string, fwf.colCount)
	for i, str := range cols {
		var err error
		formatted[i], err = fwf.FormatColumn(str, i)

		if err != nil {
			return nil, err
		}
	}

	return formatted, nil
}

func (fwf FixedWidthFormatter) FormatColumn(colStr string, colIdx int) (string, error) {
	colWidth := fwf.widths[colIdx]

	if colWidth <= 0 {
		return "", nil
	}

	strWidth := StringWidth(colStr)

	if strWidth > colWidth {
		switch fwf.tooLngBhv {
		case ErrorWhenTooLong:
			return "", fmt.Errorf("for column %d '%s' exceeds the maximum length of %d: %w", i, str, colWidth, ErrColumnTooLong)
		case TruncateWhenTooLong:
			colStr = colStr[0:colWidth]
		case HashFillWhenTooLong:
			colStr = fwf.noFitStrs[colIdx]
		case PrintAllWhenTooLong:
			break
		}
	}

	return colStr, nil
}

func (fwf FixedWidthFormatter) TotalWidth() int {
	return fwf.totalWidth
}
