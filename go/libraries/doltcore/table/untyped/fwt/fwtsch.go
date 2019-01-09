package fwt

import (
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"strings"
)

type FWTSchema struct {
	Sch          *schema.Schema
	Widths       []int
	NoFitStrs    []string
	totalWidth   int
	dispColCount int
	lastDispCol  int
}

func NewFWTSchema(sch *schema.Schema, fldToWidth map[string]int) (*FWTSchema, error) {
	widths := make([]int, sch.NumFields())
	for i := 0; i < sch.NumFields(); i++ {
		widths[i] = 0
	}

	for name, width := range fldToWidth {
		if width < 0 {
			width = 0
		}

		fldIdx := sch.GetFieldIndex(strings.ToLower(name))

		if fldIdx != -1 {
			widths[fldIdx] = width
		} else {
			return nil, errors.New("Unknown field " + name)
		}
	}

	return NewFWTSchemaWithWidths(sch, widths), nil
}

func NewFWTSchemaWithWidths(sch *schema.Schema, widths []int) *FWTSchema {
	if len(widths) != sch.NumFields() {
		panic("Invalid widths slice should have a value for every field.")
	}

	for i := 0; i < sch.NumFields(); i++ {
		fld := sch.GetField(i)
		if fld.NomsKind() != types.StringKind {
			panic("Invalid schema argument.  Has non-String fields. Use a rowconverter, or mapping reader / writer")
		}
	}

	totalWidth := 0
	dispColCount := 0
	lastDispCol := -1

	for i, width := range widths {
		if width > 0 {
			totalWidth += width
			dispColCount++
		}

		if i > lastDispCol {
			lastDispCol = i
		}
	}

	noFitStrs := make([]string, sch.NumFields())
	for i := 0; i < sch.NumFields(); i++ {
		chars := make([]byte, widths[i])
		for j := 0; j < widths[i]; j++ {
			chars[j] = '#'
		}

		noFitStrs[i] = string(chars)
	}

	return &FWTSchema{sch, widths, noFitStrs, totalWidth, dispColCount, lastDispCol}
}

func (fwtSch *FWTSchema) GetTotalWidth(charsBetweenFields int) int {
	return fwtSch.totalWidth + ((fwtSch.dispColCount - 1) * charsBetweenFields)
}
