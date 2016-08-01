// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package jsontonoms

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/suite"
)

func TestLibTestSuite(t *testing.T) {
	suite.Run(t, &LibTestSuite{})
}

type LibTestSuite struct {
	suite.Suite
}

func (suite *LibTestSuite) TestPrimitiveTypes() {
	suite.EqualValues(types.String("expected"), NomsValueFromDecodedJSON("expected", false))
	suite.EqualValues(types.Bool(false), NomsValueFromDecodedJSON(false, false))
	suite.EqualValues(types.Number(1.7), NomsValueFromDecodedJSON(1.7, false))
	suite.False(NomsValueFromDecodedJSON(1.7, false).Equals(types.Bool(true)))
}

func (suite *LibTestSuite) TestCompositeTypes() {
	// [false true]
	suite.EqualValues(
		types.NewList().Append(types.Bool(false)).Append(types.Bool(true)),
		NomsValueFromDecodedJSON([]interface{}{false, true}, false))

	// [[false true]]
	suite.EqualValues(
		types.NewList().Append(
			types.NewList().Append(types.Bool(false)).Append(types.Bool(true))),
		NomsValueFromDecodedJSON([]interface{}{[]interface{}{false, true}}, false))

	// {"string": "string",
	//  "list": [false true],
	//  "map": {"nested": "string"}
	// }
	m := types.NewMap(
		types.String("string"),
		types.String("string"),
		types.String("list"),
		types.NewList().Append(types.Bool(false)).Append(types.Bool(true)),
		types.String("map"),
		types.NewMap(
			types.String("nested"),
			types.String("string")))
	o := NomsValueFromDecodedJSON(map[string]interface{}{
		"string": "string",
		"list":   []interface{}{false, true},
		"map":    map[string]interface{}{"nested": "string"},
	}, false)

	suite.True(m.Equals(o))
}

func (suite *LibTestSuite) TestCompositeTypeWithStruct() {
	// {"string": "string",
	//  "list": [false true],
	//  "struct": {"nested": "string"}
	// }
	tstruct := types.NewStruct("", types.StructData{
		"string": types.String("string"),
		"list":   types.NewList().Append(types.Bool(false)).Append(types.Bool(true)),
		"struct": types.NewStruct("", types.StructData{
			"nested": types.String("string"),
		}),
	})
	o := NomsValueFromDecodedJSON(map[string]interface{}{
		"string": "string",
		"list":   []interface{}{false, true},
		"struct": map[string]interface{}{"nested": "string"},
	}, true)

	suite.True(tstruct.Equals(o))
}

func (suite *LibTestSuite) TestPanicOnUnsupportedType() {
	suite.Panics(func() { NomsValueFromDecodedJSON(map[int]string{1: "one"}, false) }, "Should panic on map[int]string!")
}
