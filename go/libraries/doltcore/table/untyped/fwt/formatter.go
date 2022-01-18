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
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// TooLongBehavior determines how the FWTTransformer should behave when it encounters a column that is longer than what
// it expected
type TooLongBehavior int

const (
	// ErrorWhenTooLong treats each row containing a column that is longer than expected as a bad row
	ErrorWhenTooLong TooLongBehavior = iota
	//SkipRowWhenTooLong caller can use ErrorWhenTooLong and ignore rows that return ErrColumnTooLong
	// TruncateWhenTooLong will cut off the end of columns that are too long
	TruncateWhenTooLong
	// HashFillWhenTooLong will result in ######### being printed in place of the columns that are longer than expected.
	HashFillWhenTooLong
	// PrintAllWhenTooLong will print the entire column for every row.  When this happens results will not be valid
	// fixed width text files
	PrintAllWhenTooLong
)

// ErrRowCountMismatch is returned when the number of columns does not match the expected count
var ErrRowCountMismatch = errors.New("number of columns passed to formatter does not match expected count")

// ErrColumnTooLong is returned when the width exceeds the maximum
var ErrColumnTooLong = errors.New("column width exceeded maximum width for column and TooLongBehavior is ErrorWhenTooLong")

// FixedWidthFormatter is a utility class for taking a row and generating fixed with output
type FixedWidthFormatter struct {
	colCount   int
	Widths     []int
	maxRunes   []int
	noFitStrs  []string
	TotalWidth int

	runeBuff  [][]rune
	tooLngBhv TooLongBehavior
}

// NewFixedWidthFormatter returns a new fixed width formatter
func NewFixedWidthFormatter(tooLongBhv TooLongBehavior, printWidths []int, maxRunes []int) FixedWidthFormatter {
	numCols := len(printWidths)

	totalWidth := 0
	noFitStrs := make([]string, 0, numCols)
	runeBuffs := make([][]rune, 0, numCols)
	for i, printWidth := range printWidths {
		chars := make([]byte, printWidth)
		for j := 0; j < printWidth; j++ {
			chars[j] = '#'
		}

		width := printWidth
		if width < 0 {
			width = 0
		}

		totalWidth += width
		noFitStrs = append(noFitStrs, string(chars))
		runeBuffs = append(runeBuffs, make([]rune, maxRunes[i]))
	}

	return FixedWidthFormatter{
		colCount:   numCols,
		Widths:     printWidths,
		maxRunes:   maxRunes,
		noFitStrs:  noFitStrs,
		TotalWidth: totalWidth,
		runeBuff:   runeBuffs,
		tooLngBhv:  tooLongBhv,
	}
}

// FixedWidthFormatterForSchema takes a schema and creates a FixedWidthFormatter based on the columns within that schema
func FixedWidthFormatterForSchema(sch schema.Schema, tooLongBhv TooLongBehavior, tagToPrintWidth map[uint64]int, tagToMaxRunes map[uint64]int) FixedWidthFormatter {
	allCols := sch.GetAllCols()

	if len(tagToPrintWidth) != allCols.Size() {
		panic("Invalid tagToPrintWidth map should have a value for every field.")
	}

	if len(tagToMaxRunes) != allCols.Size() {
		panic("Invalid tagToMaxRunes map should have a value for every field.")
	}

	widths := make([]int, 0, allCols.Size())
	maxRunes := make([]int, 0, allCols.Size())
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Kind != types.StringKind {
			panic("Invalid schema argument.  Has non-String fields. Use a rowconverter, or mapping reader / writer")
		}

		width := tagToPrintWidth[tag]
		if width < 0 {
			width = 0
		}

		widths = append(widths, width)
		maxRunes = append(maxRunes, tagToMaxRunes[tag])
		return false, nil
	})

	return NewFixedWidthFormatter(tooLongBhv, widths, maxRunes)
}

// FormatRow takes a row and converts it so that the columns are appropriately sized
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

// Format takes an array of columns strings and makes each column the approriate width
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

// FormatColumn takes a column string and a column index and returns a column string that is the appropriate width for
// that column
func (fwf FixedWidthFormatter) FormatColumn(colStr string, colIdx int) (string, error) {
	colWidth := fwf.Widths[colIdx]

	if colWidth <= 0 {
		return "", nil
	}

	strWidth := StringWidth(colStr)

	if strWidth > colWidth {
		switch fwf.tooLngBhv {
		case ErrorWhenTooLong:
			return "", fmt.Errorf("for column %d '%s' exceeds the maximum length of %d: %w", colIdx, colStr, colWidth, ErrColumnTooLong)
		case TruncateWhenTooLong:
			colStr = colStr[0:colWidth]
		case HashFillWhenTooLong:
			colStr = fwf.noFitStrs[colIdx]
		case PrintAllWhenTooLong:
			break
		}
	}

	buf := fwf.runeBuff[colIdx]
	strWidth = StringWidth(colStr)
	if strWidth > colWidth {
		buf = []rune(colStr)
	} else {
		n := copy(buf, []rune(colStr))
		// Character widths are tricky. Always overwrite from where we left off to the end of the buffer to clear it.
		for i := 0; n+i < len(buf); i++ {
			buf[n+i] = ' '
		}
	}

	return string(buf), nil
}
