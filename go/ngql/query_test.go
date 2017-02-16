// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package ngql

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/suite"
)

type QueryGraphQLSuite struct {
	suite.Suite
	vs *types.ValueStore
}

func TestQueryGraphQL(t *testing.T) {
	suite.Run(t, &QueryGraphQLSuite{})
}

func (suite *QueryGraphQLSuite) SetupTest() {
	cs := chunks.NewTestStore()
	suite.vs = types.NewValueStore(types.NewBatchStoreAdaptor(cs))
}

func (suite *QueryGraphQLSuite) assertQueryResult(v types.Value, q, expect string) {
	buff := &bytes.Buffer{}
	Query(v, q, suite.vs, buff)
	suite.Equal(expect, string(buff.Bytes()))
}

func (suite *QueryGraphQLSuite) TestScalars() {
	suite.assertQueryResult(types.String("aaa"), "{root}", `{"data":{"root":"aaa"}}`)
	suite.assertQueryResult(types.String(""), "{root}", `{"data":{"root":""}}`)

	suite.assertQueryResult(types.Number(0), "{root}", `{"data":{"root":0}}`)
	suite.assertQueryResult(types.Number(1), "{root}", `{"data":{"root":1}}`)
	suite.assertQueryResult(types.Number(-1), "{root}", `{"data":{"root":-1}}`)
	suite.assertQueryResult(types.Number(1<<31), "{root}", `{"data":{"root":2.147483648e+09}}`)
	suite.assertQueryResult(types.Number(-(1 << 31)), "{root}", `{"data":{"root":-2.147483648e+09}}`)
	suite.assertQueryResult(types.Number(0.001), "{root}", `{"data":{"root":0.001}}`)
	suite.assertQueryResult(types.Number(0.00000001), "{root}", `{"data":{"root":1e-08}}`)

	suite.assertQueryResult(types.Bool(false), "{root}", `{"data":{"root":false}}`)
	suite.assertQueryResult(types.Bool(true), "{root}", `{"data":{"root":true}}`)
}

func (suite *QueryGraphQLSuite) TestStructBasic() {
	s1 := types.NewStruct("Foo", types.StructData{
		"a": types.String("aaa"),
		"b": types.Bool(true),
		"c": types.Number(0.1),
	})

	suite.assertQueryResult(s1, "{root{a}}", `{"data":{"root":{"a":"aaa"}}}`)
	suite.assertQueryResult(s1, "{root{a b}}", `{"data":{"root":{"a":"aaa","b":true}}}`)
	suite.assertQueryResult(s1, "{root{a b c}}", `{"data":{"root":{"a":"aaa","b":true,"c":0.1}}}`)
	suite.assertQueryResult(s1, "{root{a c}}", `{"data":{"root":{"a":"aaa","c":0.1}}}`)
}

func (suite *QueryGraphQLSuite) TestEmptyStruct() {
	s1 := types.NewStruct("", types.StructData{})

	suite.assertQueryResult(s1, "{root{hash}}", `{"data":{"root":{"hash":"c66c33bb6na2m5mk0bek7eqqrl2t7gmv"}}}`)
}

func (suite *QueryGraphQLSuite) TestEmbeddedStruct() {
	s1 := types.NewStruct("Foo", types.StructData{
		"a": types.String("aaa"),
		"b": types.NewStruct("Bar", types.StructData{
			"c": types.Bool(true),
			"d": types.Number(0.1),
		}),
	})

	suite.assertQueryResult(s1, "{root{a}}", `{"data":{"root":{"a":"aaa"}}}`)
	suite.assertQueryResult(s1, "{root{a b {c}}}", `{"data":{"root":{"a":"aaa","b":{"c":true}}}}`)
	suite.assertQueryResult(s1, "{root{a b {c d}}}", `{"data":{"root":{"a":"aaa","b":{"c":true,"d":0.1}}}}`)
}

func (suite *QueryGraphQLSuite) TestListBasic() {
	list := types.NewList()
	suite.assertQueryResult(list, "{root{size}}", `{"data":{"root":{"size":0}}}`)
	suite.assertQueryResult(list, "{root{elements}}", `{"data":null,"errors":[{"message":"Cannot query field \"elements\" on type \"EmptyList\".","locations":[{"line":1,"column":7}]}]}`)

	list = types.NewList(types.String("foo"), types.String("bar"), types.String("baz"))

	suite.assertQueryResult(list, "{root{elements}}", `{"data":{"root":{"elements":["foo","bar","baz"]}}}`)
	suite.assertQueryResult(list, "{root{size}}", `{"data":{"root":{"size":3}}}`)
	suite.assertQueryResult(list, "{root{elements(at:1,count:2)}}", `{"data":{"root":{"elements":["bar","baz"]}}}`)

	list = types.NewList(types.Bool(true), types.Bool(false), types.Bool(false))

	suite.assertQueryResult(list, "{root{elements}}", `{"data":{"root":{"elements":[true,false,false]}}}`)
	suite.assertQueryResult(list, "{root{elements(at:1,count:2)}}", `{"data":{"root":{"elements":[false,false]}}}`)

	list = types.NewList(types.Number(1), types.Number(1.1), types.Number(-100))

	suite.assertQueryResult(list, "{root{elements}}", `{"data":{"root":{"elements":[1,1.1,-100]}}}`)
	suite.assertQueryResult(list, "{root{elements(at:1,count:2)}}", `{"data":{"root":{"elements":[1.1,-100]}}}`)
}

func (suite *QueryGraphQLSuite) TestListOfStruct() {
	list := types.NewList(
		types.NewStruct("Foo", types.StructData{
			"a": types.Number(28),
			"b": types.String("foo"),
		}),
		types.NewStruct("Foo", types.StructData{
			"a": types.Number(-20.102),
			"b": types.String("bar"),
		}),
		types.NewStruct("Foo", types.StructData{
			"a": types.Number(5),
			"b": types.String("baz"),
		}),
	)

	suite.assertQueryResult(list, "{root{elements{a b}}}", `{"data":{"root":{"elements":[{"a":28,"b":"foo"},{"a":-20.102,"b":"bar"},{"a":5,"b":"baz"}]}}}`)

	suite.assertQueryResult(list, "{root{elements{a}}}", `{"data":{"root":{"elements":[{"a":28},{"a":-20.102},{"a":5}]}}}`)
}

func (suite *QueryGraphQLSuite) TestSetBasic() {
	set := types.NewSet()
	suite.assertQueryResult(set, "{root{size}}", `{"data":{"root":{"size":0}}}`)
	suite.assertQueryResult(set, "{root{elements}}", `{"data":null,"errors":[{"message":"Cannot query field \"elements\" on type \"EmptySet\".","locations":[{"line":1,"column":7}]}]}`)

	set = types.NewSet(types.String("foo"), types.String("bar"), types.String("baz"))

	suite.assertQueryResult(set, "{root{elements}}", `{"data":{"root":{"elements":["bar","baz","foo"]}}}`)
	suite.assertQueryResult(set, "{root{size}}", `{"data":{"root":{"size":3}}}`)
	suite.assertQueryResult(set, "{root{elements(count:2)}}", `{"data":{"root":{"elements":["bar","baz"]}}}`)

	set = types.NewSet(types.Bool(true), types.Bool(false))

	suite.assertQueryResult(set, "{root{elements}}", `{"data":{"root":{"elements":[false,true]}}}`)
	suite.assertQueryResult(set, "{root{elements(count:1)}}", `{"data":{"root":{"elements":[false]}}}`)

	set = types.NewSet(types.Number(1), types.Number(1.1), types.Number(-100))

	suite.assertQueryResult(set, "{root{elements}}", `{"data":{"root":{"elements":[-100,1,1.1]}}}`)
	suite.assertQueryResult(set, "{root{elements(count:2)}}", `{"data":{"root":{"elements":[-100,1]}}}`)
}

func (suite *QueryGraphQLSuite) TestSetOfStruct() {
	set := types.NewSet(
		types.NewStruct("Foo", types.StructData{
			"a": types.Number(28),
			"b": types.String("foo"),
		}),
		types.NewStruct("Foo", types.StructData{
			"a": types.Number(-20.102),
			"b": types.String("bar"),
		}),
		types.NewStruct("Foo", types.StructData{
			"a": types.Number(5),
			"b": types.String("baz"),
		}),
	)

	suite.assertQueryResult(set, "{root{elements{a b}}}", `{"data":{"root":{"elements":[{"a":-20.102,"b":"bar"},{"a":5,"b":"baz"},{"a":28,"b":"foo"}]}}}`)
	suite.assertQueryResult(set, "{root{elements{a}}}", `{"data":{"root":{"elements":[{"a":-20.102},{"a":5},{"a":28}]}}}`)
}

func (suite *QueryGraphQLSuite) TestMapBasic() {
	m := types.NewMap()
	suite.assertQueryResult(m, "{root{size}}", `{"data":{"root":{"size":0}}}`)
	suite.assertQueryResult(m, "{root{elements}}", `{"data":null,"errors":[{"message":"Cannot query field \"elements\" on type \"EmptyMap\".","locations":[{"line":1,"column":7}]}]}`)

	m = types.NewMap(
		types.String("foo"), types.Number(1),
		types.String("bar"), types.Number(2),
		types.String("baz"), types.Number(3),
	)

	suite.assertQueryResult(m, "{root{elements{key value}}}", `{"data":{"root":{"elements":[{"key":"bar","value":2},{"key":"baz","value":3},{"key":"foo","value":1}]}}}`)
	suite.assertQueryResult(m, "{root{size}}", `{"data":{"root":{"size":3}}}`)
	suite.assertQueryResult(m, "{root{elements(count:2){value}}}", `{"data":{"root":{"elements":[{"value":2},{"value":3}]}}}`)
	suite.assertQueryResult(m, "{root{elements(count:3){key}}}", `{"data":{"root":{"elements":[{"key":"bar"},{"key":"baz"},{"key":"foo"}]}}}`)
}

func (suite *QueryGraphQLSuite) TestMapOfStruct() {
	m := types.NewMap(
		types.String("foo"), types.NewStruct("Foo", types.StructData{
			"a": types.Number(28),
			"b": types.String("foo"),
		}),
		types.String("bar"), types.NewStruct("Foo", types.StructData{
			"a": types.Number(-20.102),
			"b": types.String("bar"),
		}),
		types.String("baz"), types.NewStruct("Foo", types.StructData{
			"a": types.Number(5),
			"b": types.String("baz"),
		}),
	)

	suite.assertQueryResult(m, "{root{elements{key value{a}}}}", `{"data":{"root":{"elements":[{"key":"bar","value":{"a":-20.102}},{"key":"baz","value":{"a":5}},{"key":"foo","value":{"a":28}}]}}}`)
	suite.assertQueryResult(m, "{root{elements(count:1){value{a b}}}}", `{"data":{"root":{"elements":[{"value":{"a":-20.102,"b":"bar"}}]}}}`)
	suite.assertQueryResult(m, "{root{elements(count:3){key}}}", `{"data":{"root":{"elements":[{"key":"bar"},{"key":"baz"},{"key":"foo"}]}}}`)
}

func (suite *QueryGraphQLSuite) TestRef() {
	r := suite.vs.WriteValue(types.Number(100))

	suite.assertQueryResult(r, "{root{targetValue}}", `{"data":{"root":{"targetValue":100}}}`)
	suite.assertQueryResult(r, "{root{targetHash}}", `{"data":{"root":{"targetHash":"fpbhln9asjlalp10btna9ocuc4nj9v15"}}}`)
	suite.assertQueryResult(r, "{root{targetValue targetHash}}", `{"data":{"root":{"targetHash":"fpbhln9asjlalp10btna9ocuc4nj9v15","targetValue":100}}}`)

	r = suite.vs.WriteValue(types.NewStruct("Foo", types.StructData{
		"a": types.Number(28),
		"b": types.String("foo"),
	}))

	suite.assertQueryResult(r, "{root{targetValue{a}}}", `{"data":{"root":{"targetValue":{"a":28}}}}`)
	suite.assertQueryResult(r, "{root{targetValue{a b}}}", `{"data":{"root":{"targetValue":{"a":28,"b":"foo"}}}}`)

	r = suite.vs.WriteValue(types.NewList(types.String("foo"), types.String("bar"), types.String("baz")))

	suite.assertQueryResult(r, "{root{targetValue{elements}}}", `{"data":{"root":{"targetValue":{"elements":["foo","bar","baz"]}}}}`)
	suite.assertQueryResult(r, "{root{targetValue{elements(at:1,count:2)}}}", `{"data":{"root":{"targetValue":{"elements":["bar","baz"]}}}}`)
}

func (suite *QueryGraphQLSuite) TestListOfUnionOfStructs() {
	list := types.NewList(
		types.NewStruct("Foo", types.StructData{
			"a": types.Number(28),
			"b": types.String("baz"),
		}),
		types.NewStruct("Bar", types.StructData{
			"b": types.String("bar"),
		}),
		types.NewStruct("Baz", types.StructData{
			"c": types.Bool(true),
		}),
	)

	suite.assertQueryResult(list,
		fmt.Sprintf("{root{elements{... on %s{a b} ... on %s{b} ... on %s{c}}}}",
			getTypeName(list.Get(0).Type()),
			getTypeName(list.Get(1).Type()),
			getTypeName(list.Get(2).Type())),
		`{"data":{"root":{"elements":[{"a":28,"b":"baz"},{"b":"bar"},{"c":true}]}}}`)
}

func (suite *QueryGraphQLSuite) TestListOfUnionOfStructsConflictingFieldTypes() {
	list := types.NewList(
		types.NewStruct("Foo", types.StructData{
			"a": types.Number(28),
		}),
		types.NewStruct("Bar", types.StructData{
			"a": types.String("bar"),
		}),
		types.NewStruct("Baz", types.StructData{
			"a": types.Bool(true),
		}),
	)

	suite.assertQueryResult(list,
		fmt.Sprintf("{root{elements{... on %s{a} ... on %s{b: a} ... on %s{c: a}}}}",
			getTypeName(list.Get(0).Type()),
			getTypeName(list.Get(1).Type()),
			getTypeName(list.Get(2).Type())),
		`{"data":{"root":{"elements":[{"a":28},{"b":"bar"},{"c":true}]}}}`)
}

func (suite *QueryGraphQLSuite) TestListOfUnionOfScalars() {
	list := types.NewList(
		types.Number(28),
		types.String("bar"),
		types.Bool(true),
	)

	suite.assertQueryResult(list, "{root{elements{... on BooleanValue{b: scalarValue} ... on StringValue{s: scalarValue} ... on NumberValue{n: scalarValue}}}}", `{"data":{"root":{"elements":[{"n":28},{"s":"bar"},{"b":true}]}}}`)
}

func (suite *QueryGraphQLSuite) TestCyclicStructs() {
	typ := types.MakeStructTypeFromFields("A", types.FieldMap{
		"a": types.StringType,
		"b": types.MakeSetType(types.MakeCycleType(0)),
	})

	// Struct A {
	//  a: "aaa"
	//  b: Set(Struct A {
	// 	 a: "bbb"
	// 	 b: Set()
	//  })
	// }

	s1 := types.NewStructWithType(typ, types.ValueSlice{
		types.String("aaa"),
		types.NewSet(types.NewStructWithType(typ, types.ValueSlice{types.String("bbb"), types.NewSet()})),
	})

	suite.assertQueryResult(s1, "{root{a b{elements{a}}}}", `{"data":{"root":{"a":"aaa","b":{"elements":[{"a":"bbb"}]}}}}`)
}

func (suite *QueryGraphQLSuite) TestNestedCollection() {
	list := types.NewList(
		types.NewSet(
			types.NewMap(types.Number(10), types.String("foo")),
			types.NewMap(types.Number(20), types.String("bar")),
		),
		types.NewSet(
			types.NewMap(types.Number(30), types.String("baz")),
			types.NewMap(types.Number(40), types.String("bat")),
		),
	)

	suite.assertQueryResult(list, "{root{size}}", `{"data":{"root":{"size":2}}}`)
	suite.assertQueryResult(list, "{root{elements(count:1){size}}}", `{"data":{"root":{"elements":[{"size":2}]}}}`)
	suite.assertQueryResult(list, "{root{elements(at:1,count:1){elements(count:1){elements{key value}}}}}", `{"data":{"root":{"elements":[{"elements":[{"elements":[{"key":30,"value":"baz"}]}]}]}}}`)
}

func (suite *QueryGraphQLSuite) TestLoFi() {
	b := types.NewBlob(bytes.NewBufferString("I am a blob"))

	suite.assertQueryResult(b, "{root}", `{"data":{"root":"h6jkv35uum62a7ovu14uvmhaf0sojgh6"}}`)

	t := types.StringType
	suite.assertQueryResult(t, "{root}", `{"data":{"root":"pej65tf21rubhu9cb0oi5gqrkgf26aql"}}`)
}
