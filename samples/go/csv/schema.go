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

func GetSchema(r *csv.Reader, numSamples int, numFields int) KindSlice {
	so := newSchemaOptions(numFields)
	for i := 0; i < numSamples; i++ {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		so.Test(row)
	}
	return so.MostSpecificKinds()
}

func GetFieldNamesFromIndices(headers []string, indices []int) []string {
	result := make([]string, len(indices))
	for i, idx := range indices {
		result[i] = headers[idx]
	}
	return result
}

// combinations - n choose m combination without repeat - emit all possible `length` combinations from values
func combinationsWithLength(values []int, length int, emit func([]int)) {
	n := len(values)

	if length > n {
		return
	}

	indices := make([]int, length)
	for i := range indices {
		indices[i] = i
	}

	result := make([]int, length)
	for i, l := range indices {
		result[i] = values[l]
	}
	emit(result)

	for {
		i := length - 1
		for ; i >= 0 && indices[i] == i+n-length; i -= 1 {
		}

		if i < 0 {
			return
		}

		indices[i] += 1
		for j := i + 1; j < length; j += 1 {
			indices[j] = indices[j-1] + 1
		}

		for ; i < len(indices); i += 1 {
			result[i] = values[indices[i]]
		}
		emit(result)
	}
}

// combinationsLengthsFromTo - n choose m combination without repeat - emit all possible combinations of all lengths from smallestLength to largestLength (inclusive)
func combinationsLengthsFromTo(values []int, smallestLength, largestLength int, emit func([]int)) {
	for i := smallestLength; i <= largestLength; i++ {
		combinationsWithLength(values, i, emit)
	}
}

func makeKeyString(row []string, indices []int, separator string) string {
	var result string
	for _, i := range indices {
		result += separator
		result += row[i]
	}
	return result
}

// FindPrimaryKeys reads numSamples from r, using the first numFields and returns slices of []int indices that are primary keys for those samples
func FindPrimaryKeys(r *csv.Reader, numSamples, maxLenPrimaryKeyList, numFields int) [][]int {
	dataToTest := make([][]string, 0, numSamples)
	for i := int(0); i < numSamples; i++ {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		dataToTest = append(dataToTest, row)
	}

	indices := make([]int, numFields)
	for i := int(0); i < numFields; i++ {
		indices[i] = i
	}

	pksFound := make([][]int, 0)
	combinationsLengthsFromTo(indices, 1, maxLenPrimaryKeyList, func(combination []int) {
		keys := make(map[string]bool, numSamples)
		for _, row := range dataToTest {
			key := makeKeyString(row, combination, "$&$")
			if _, ok := keys[key]; ok {
				return
			}
			keys[key] = true
		}
		// need to copy the combination because it will be changed by caller
		pksFound = append(pksFound, append([]int{}, combination...))
	})
	return pksFound
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
		d.Panic("Invalid column type kind:", k)
	}
	panic("not reached")
}
