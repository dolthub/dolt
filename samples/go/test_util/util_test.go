// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package test_util

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/samples/go/util"
	"github.com/attic-labs/testify/suite"
)

func TestLibTestSuite(t *testing.T) {
	suite.Run(t, &LibTestSuite{})
}

type LibTestSuite struct {
	suite.Suite
}

func (suite *LibTestSuite) TestPrimitiveTypes() {
	suite.EqualValues(types.NewString("expected"), util.NomsValueFromDecodedJSON("expected", false))
	suite.EqualValues(types.Bool(false), util.NomsValueFromDecodedJSON(false, false))
	suite.EqualValues(types.Number(1.7), util.NomsValueFromDecodedJSON(1.7, false))
	suite.False(util.NomsValueFromDecodedJSON(1.7, false).Equals(types.Bool(true)))
}

func (suite *LibTestSuite) TestCompositeTypes() {
	// [false true]
	suite.EqualValues(
		types.NewList().Append(types.Bool(false)).Append(types.Bool(true)),
		util.NomsValueFromDecodedJSON([]interface{}{false, true}, false))

	// [[false true]]
	suite.EqualValues(
		types.NewList().Append(
			types.NewList().Append(types.Bool(false)).Append(types.Bool(true))),
		util.NomsValueFromDecodedJSON([]interface{}{[]interface{}{false, true}}, false))

	// {"string": "string",
	//  "list": [false true],
	//  "map": {"nested": "string"}
	// }
	m := types.NewMap(
		types.NewString("string"),
		types.NewString("string"),
		types.NewString("list"),
		types.NewList().Append(types.Bool(false)).Append(types.Bool(true)),
		types.NewString("map"),
		types.NewMap(
			types.NewString("nested"),
			types.NewString("string")))
	o := util.NomsValueFromDecodedJSON(map[string]interface{}{
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
	tstruct := types.NewStruct("", map[string]types.Value{
		"string": types.NewString("string"),
		"list":   types.NewList().Append(types.Bool(false)).Append(types.Bool(true)),
		"struct": types.NewStruct("", map[string]types.Value{
			"nested": types.NewString("string"),
		}),
	})
	o := util.NomsValueFromDecodedJSON(map[string]interface{}{
		"string": "string",
		"list":   []interface{}{false, true},
		"struct": map[string]interface{}{"nested": "string"},
	}, true)

	suite.True(tstruct.Equals(o))
}

func (suite *LibTestSuite) TestPanicOnUnsupportedType() {
	suite.Panics(func() { util.NomsValueFromDecodedJSON(map[int]string{1: "one"}, false) }, "Should panic on map[int]string!")
}
