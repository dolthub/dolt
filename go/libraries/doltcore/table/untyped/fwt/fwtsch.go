package fwt

import (
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

// FWTSchema is a fixed width text schema which includes information on a tables rows, and how wide they should be printed
type FWTSchema struct {
	Sch          schema.Schema
	TagToWidth   map[uint64]int
	NoFitStrs    map[uint64]string
	totalWidth   int
	dispColCount int
}

// NewFWTSchema creates a FWTSchema given a standard schema and a map from column name to the width of that column.
func NewFWTSchema(sch schema.Schema, fldToWidth map[string]int) (*FWTSchema, error) {
	allCols := sch.GetAllCols()
	tagToWidth := make(map[uint64]int, allCols.Size())
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		tagToWidth[tag] = 0
		return false
	})

	for name, width := range fldToWidth {
		if width < 0 {
			width = 0
		}

		col, ok := allCols.GetByName(name)

		if ok {
			tagToWidth[col.Tag] = width
		} else {
			return nil, errors.New("Unknown field " + name)
		}
	}

	return NewFWTSchemaWithWidths(sch, tagToWidth), nil
}

// NewFWTSchemaWithWidths creates a FWTSchema given a standard schema and a map from column tag to the width of that column
func NewFWTSchemaWithWidths(sch schema.Schema, tagToWidth map[uint64]int) *FWTSchema {
	allCols := sch.GetAllCols()

	if len(tagToWidth) != allCols.Size() {
		panic("Invalid widths map should have a value for every field.")
	}

	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		if col.Kind != types.StringKind {
			panic("Invalid schema argument.  Has non-String fields. Use a rowconverter, or mapping reader / writer")
		}

		return false
	})

	totalWidth := 0
	dispColCount := 0

	for _, width := range tagToWidth {
		if width > 0 {
			totalWidth += width
			dispColCount++
		}
	}

	noFitStrs := make(map[uint64]string, allCols.Size())
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		chars := make([]byte, tagToWidth[tag])
		for j := 0; j < tagToWidth[tag]; j++ {
			chars[j] = '#'
		}

		noFitStrs[tag] = string(chars)
		return false
	})

	return &FWTSchema{sch, tagToWidth, noFitStrs, totalWidth, dispColCount}
}

// GetTotalWidth returns the total width of all the columns
func (fwtSch *FWTSchema) GetTotalWidth(charsBetweenFields int) int {
	return fwtSch.totalWidth + ((fwtSch.dispColCount - 1) * charsBetweenFields)
}
