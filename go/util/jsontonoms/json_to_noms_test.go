// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package jsontonoms

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/suite"
)

func TestLibTestSuite(t *testing.T) {
	suite.Run(t, &LibTestSuite{})
}

type LibTestSuite struct {
	suite.Suite
	vs *types.ValueStore
}

func (suite *LibTestSuite) SetupTest() {
	st := &chunks.TestStorage{}
	suite.vs = types.NewValueStore(st.NewView())
}

func (suite *LibTestSuite) TearDownTest() {
	suite.vs.Close()
}

func (suite *LibTestSuite) TestPrimitiveTypes() {
	vs := suite.vs
	suite.EqualValues(types.String("expected"), NomsValueFromDecodedJSON(vs, "expected", false))
	suite.EqualValues(types.Bool(false), NomsValueFromDecodedJSON(vs, false, false))
	suite.EqualValues(types.Float(1.7), NomsValueFromDecodedJSON(vs, 1.7, false))
	suite.False(NomsValueFromDecodedJSON(vs, 1.7, false).Equals(types.Bool(true)))
}

func (suite *LibTestSuite) TestCompositeTypes() {
	vs := suite.vs

	// [false true]
	suite.EqualValues(
		types.NewList(vs).Edit().Append(types.Bool(false)).Append(types.Bool(true)).List(),
		NomsValueFromDecodedJSON(vs, []interface{}{false, true}, false))

	// [[false true]]
	suite.EqualValues(
		types.NewList(vs).Edit().Append(
			types.NewList(vs).Edit().Append(types.Bool(false)).Append(types.Bool(true)).List()).List(),
		NomsValueFromDecodedJSON(vs, []interface{}{[]interface{}{false, true}}, false))

	// {"string": "string",
	//  "list": [false true],
	//  "map": {"nested": "string"}
	// }
	m := types.NewMap(
		vs,
		types.String("string"),
		types.String("string"),
		types.String("list"),
		types.NewList(vs).Edit().Append(types.Bool(false)).Append(types.Bool(true)).List(),
		types.String("map"),
		types.NewMap(
			vs,
			types.String("nested"),
			types.String("string")))
	o := NomsValueFromDecodedJSON(vs, map[string]interface{}{
		"string": "string",
		"list":   []interface{}{false, true},
		"map":    map[string]interface{}{"nested": "string"},
	}, false)

	suite.True(m.Equals(o))
}

func (suite *LibTestSuite) TestCompositeTypeWithStruct() {
	vs := suite.vs

	// {"string": "string",
	//  "list": [false true],
	//  "struct": {"nested": "string"}
	// }
	tstruct := types.NewStruct("", types.StructData{
		"string": types.String("string"),
		"list":   types.NewList(vs).Edit().Append(types.Bool(false)).Append(types.Bool(true)).List(),
		"struct": types.NewStruct("", types.StructData{
			"nested": types.String("string"),
		}),
	})
	o := NomsValueFromDecodedJSON(vs, map[string]interface{}{
		"string": "string",
		"list":   []interface{}{false, true},
		"struct": map[string]interface{}{"nested": "string"},
	}, true)

	suite.True(tstruct.Equals(o))
}

func (suite *LibTestSuite) TestCompositeTypeWithNamedStruct() {
	vs := suite.vs

	// {
	//  "_name": "TStruct1",
	//  "string": "string",
	//  "list": [false true],
	//  "id": {"_name", "Id", "owner": "string", "value": "string"}
	// }
	tstruct := types.NewStruct("TStruct1", types.StructData{
		"string": types.String("string"),
		"list":   types.NewList(vs).Edit().Append(types.Bool(false)).Append(types.Bool(true)).List(),
		"struct": types.NewStruct("Id", types.StructData{
			"owner": types.String("string"),
			"value": types.String("string"),
		}),
	})
	o := NomsValueUsingNamedStructsFromDecodedJSON(vs, map[string]interface{}{
		"_name":  "TStruct1",
		"string": "string",
		"list":   []interface{}{false, true},
		"struct": map[string]interface{}{"_name": "Id", "owner": "string", "value": "string"},
	})

	suite.True(tstruct.Equals(o))
}

func (suite *LibTestSuite) TestPanicOnUnsupportedType() {
	vs := suite.vs
	suite.Panics(func() { NomsValueFromDecodedJSON(vs, map[int]string{1: "one"}, false) }, "Should panic on map[int]string!")
}
