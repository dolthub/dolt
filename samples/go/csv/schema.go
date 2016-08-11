// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

type schemaOptions []*typeCanFit

func newSchemaOptions(fieldCount int) schemaOptions {
	options := make([]*typeCanFit, fieldCount, fieldCount)
	for i := 0; i < fieldCount; i++ {
		options[i] = &typeCanFit{true, true, true}
	}
	return options
}

func (so schemaOptions) Test(fields []string) {
	for i, t := range so {
		if i < len(fields) {
			t.Test(fields[i])
		}
	}
}

func (so schemaOptions) MostSpecificKinds() KindSlice {
	kinds := make(KindSlice, len(so))
	for i, t := range so {
		kinds[i] = t.MostSpecificKind()
	}
	return kinds
}

func (so schemaOptions) ValidKinds() []KindSlice {
	kinds := make([]KindSlice, len(so))
	for i, t := range so {
		kinds[i] = t.ValidKinds()
	}
	return kinds
}

type typeCanFit struct {
	boolType   bool
	numberType bool
	stringType bool
}

func (tc *typeCanFit) MostSpecificKind() types.NomsKind {
	if tc.boolType {
		return types.BoolKind
	} else if tc.numberType {
		return types.NumberKind
	} else {
		return types.StringKind
	}
}

func (tc *typeCanFit) ValidKinds() (kinds KindSlice) {
	if tc.numberType {
		kinds = append(kinds, types.NumberKind)
	}
	if tc.boolType {
		kinds = append(kinds, types.BoolKind)
	}
	kinds = append(kinds, types.StringKind)
	return kinds
}

func (tc *typeCanFit) Test(value string) {
	tc.testNumbers(value)
	tc.testBool(value)
}

func (tc *typeCanFit) testNumbers(value string) {
	if !tc.numberType {
		return
	}

	fval, err := strconv.ParseFloat(value, 64)
	if err != nil {
		tc.numberType = false
		return
	}

	if fval > math.MaxFloat64 {
		tc.numberType = false
	}
}

func (tc *typeCanFit) testBool(value string) {
	if !tc.boolType {
		return
	}
	_, err := strconv.ParseBool(value)
	tc.boolType = err == nil
}

func GetSchema(r *csv.Reader, headers []string) KindSlice {
	so := newSchemaOptions(len(headers))
	for i := 0; i < 100; i++ {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		so.Test(row)
	}
	return so.MostSpecificKinds()
}

// StringToValue takes a piece of data as a string and attempts to convert it to a types.Value of the appropriate types.NomsKind.
func StringToValue(s string, k types.NomsKind) (types.Value, error) {
	switch k {
	case types.NumberKind:
		if s == "" {
			return types.Number(float64(0)), nil
		}
		fval, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("Could not parse '%s' into number (%s)", s, err)
		}
		return types.Number(fval), nil
	case types.BoolKind:
		// TODO: This should probably be configurable.
		switch s {
		case "true", "1", "y", "Y":
			return types.Bool(true), nil
		case "false", "0", "n", "N", "":
			return types.Bool(false), nil
		default:
			return nil, fmt.Errorf("Could not parse '%s' into bool", s)
		}
	case types.StringKind:
		return types.String(s), nil
	default:
		d.PanicIfTrue(true, "Invalid column type kind:", k)
	}
	panic("not reached")
}
