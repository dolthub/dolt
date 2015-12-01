package util

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/types"
)

func TestLibTestSuite(t *testing.T) {
	suite.Run(t, &LibTestSuite{})
}

type LibTestSuite struct {
	suite.Suite
}

func (suite *LibTestSuite) TestPrimitiveTypes() {
	cs := chunks.NewMemoryStore()
	suite.EqualValues(types.NewString("expected"), NomsValueFromDecodedJSON(cs, "expected"))
	suite.EqualValues(types.Bool(false), NomsValueFromDecodedJSON(cs, false))
	suite.EqualValues(types.Float64(1.7), NomsValueFromDecodedJSON(cs, 1.7))
	suite.False(NomsValueFromDecodedJSON(cs, 1.7).Equals(types.Bool(true)))
}

func (suite *LibTestSuite) TestCompositeTypes() {
	cs := chunks.NewMemoryStore()
	// [false true]
	suite.EqualValues(
		types.NewList(cs).Append(types.Bool(false)).Append(types.Bool(true)),
		NomsValueFromDecodedJSON(cs, []interface{}{false, true}))

	// [[false true]]
	suite.EqualValues(
		types.NewList(cs).Append(
			types.NewList(cs).Append(types.Bool(false)).Append(types.Bool(true))),
		NomsValueFromDecodedJSON(cs, []interface{}{[]interface{}{false, true}}))

	// {"string": "string",
	//  "list": [false true],
	//  "map": {"nested": "string"}
	// }
	m := MapOfStringToValueDef{
		"string": types.NewString("string"),
		"list":   types.NewList(cs).Append(types.Bool(false)).Append(types.Bool(true)),
		"map":    MapOfStringToValueDef{"nested": types.NewString("string")}.New(cs),
	}.New(cs)
	o := NomsValueFromDecodedJSON(cs, map[string]interface{}{
		"string": "string",
		"list":   []interface{}{false, true},
		"map":    map[string]interface{}{"nested": "string"},
	})

	suite.True(m.Equals(o))
}

func (suite *LibTestSuite) TestPanicOnUnsupportedType() {
	cs := chunks.NewMemoryStore()
	suite.Panics(func() { NomsValueFromDecodedJSON(cs, map[int]string{1: "one"}) }, "Should panic on map[int]string!")
}
