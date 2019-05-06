// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package ngql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type QueryGraphQLSuite struct {
	suite.Suite
	vs *types.ValueStore
}

func TestQueryGraphQL(t *testing.T) {
	suite.Run(t, &QueryGraphQLSuite{})
}

func newTestValueStore() *types.ValueStore {
	storage := &chunks.MemoryStorage{}
	return types.NewValueStore(storage.NewView())
}

func (suite *QueryGraphQLSuite) SetupTest() {
	suite.vs = newTestValueStore()
}

func (suite *QueryGraphQLSuite) assertQueryResult(v types.Value, q, expect string) {
	buf := &bytes.Buffer{}
	Query(context.Background(), v, q, suite.vs, buf)
	suite.JSONEq(test.RemoveHashes(expect), test.RemoveHashes(buf.String()))
}

func (suite *QueryGraphQLSuite) TestScalars() {
	suite.assertQueryResult(types.String("aaa"), "{root}", `{"data":{"root":"aaa"}}`)
	suite.assertQueryResult(types.String(""), "{root}", `{"data":{"root":""}}`)

	suite.assertQueryResult(types.Float(0), "{root}", `{"data":{"root":0}}`)
	suite.assertQueryResult(types.Float(1), "{root}", `{"data":{"root":1}}`)
	suite.assertQueryResult(types.Float(-1), "{root}", `{"data":{"root":-1}}`)
	suite.assertQueryResult(types.Float(1<<31), "{root}", `{"data":{"root":2.147483648e+09}}`)
	suite.assertQueryResult(types.Float(-(1 << 31)), "{root}", `{"data":{"root":-2.147483648e+09}}`)
	suite.assertQueryResult(types.Float(0.001), "{root}", `{"data":{"root":0.001}}`)
	suite.assertQueryResult(types.Float(0.00000001), "{root}", `{"data":{"root":1e-08}}`)

	suite.assertQueryResult(types.Bool(false), "{root}", `{"data":{"root":false}}`)
	suite.assertQueryResult(types.Bool(true), "{root}", `{"data":{"root":true}}`)
}

func (suite *QueryGraphQLSuite) TestStructBasic() {
	s1 := types.NewStruct("Foo", types.StructData{
		"a": types.String("aaa"),
		"b": types.Bool(true),
		"c": types.Float(0.1),
	})

	suite.assertQueryResult(s1, "{root{a}}", `{"data":{"root":{"a":"aaa"}}}`)
	suite.assertQueryResult(s1, "{root{a b}}", `{"data":{"root":{"a":"aaa","b":true}}}`)
	suite.assertQueryResult(s1, "{root{a b c}}", `{"data":{"root":{"a":"aaa","b":true,"c":0.1}}}`)
	suite.assertQueryResult(s1, "{root{a c}}", `{"data":{"root":{"a":"aaa","c":0.1}}}`)
}

func (suite *QueryGraphQLSuite) TestEmptyStruct() {
	s1 := types.NewStruct("", types.StructData{})

	suite.assertQueryResult(s1, "{root{hash}}", `{"data":{"root":{"hash":"0123456789abcdefghijklmnopqrstuv"}}}`)
}

func (suite *QueryGraphQLSuite) TestEmbeddedStruct() {
	s1 := types.NewStruct("Foo", types.StructData{
		"a": types.String("aaa"),
		"b": types.NewStruct("Bar", types.StructData{
			"c": types.Bool(true),
			"d": types.Float(0.1),
		}),
	})

	suite.assertQueryResult(s1, "{root{a}}", `{"data":{"root":{"a":"aaa"}}}`)
	suite.assertQueryResult(s1, "{root{a b {c}}}", `{"data":{"root":{"a":"aaa","b":{"c":true}}}}`)
	suite.assertQueryResult(s1, "{root{a b {c d}}}", `{"data":{"root":{"a":"aaa","b":{"c":true,"d":0.1}}}}`)
}

func (suite *QueryGraphQLSuite) TestListBasic() {
	for _, valuesKey := range []string{"elements", "values"} {
		list := types.NewList(context.Background(), suite.vs)
		suite.assertQueryResult(list, "{root{size}}", `{"data":{"root":{"size":0}}}`)
		suite.assertQueryResult(list, "{root{"+valuesKey+"}}", `{"data":{"root":{}}}`)

		list = types.NewList(context.Background(), suite.vs, types.String("foo"), types.String("bar"), types.String("baz"))

		suite.assertQueryResult(list, "{root{"+valuesKey+"}}", `{"data":{"root":{"`+valuesKey+`":["foo","bar","baz"]}}}`)
		suite.assertQueryResult(list, "{root{size}}", `{"data":{"root":{"size":3}}}`)
		suite.assertQueryResult(list, "{root{"+valuesKey+"(at:1,count:2)}}", `{"data":{"root":{"`+valuesKey+`":["bar","baz"]}}}`)

		list = types.NewList(context.Background(), suite.vs, types.Bool(true), types.Bool(false), types.Bool(false))

		suite.assertQueryResult(list, "{root{"+valuesKey+"}}", `{"data":{"root":{"`+valuesKey+`":[true,false,false]}}}`)
		suite.assertQueryResult(list, "{root{"+valuesKey+"(at:1,count:2)}}", `{"data":{"root":{"`+valuesKey+`":[false,false]}}}`)

		list = types.NewList(context.Background(), suite.vs, types.Float(1), types.Float(1.1), types.Float(-100))

		suite.assertQueryResult(list, "{root{"+valuesKey+"}}", `{"data":{"root":{"`+valuesKey+`":[1,1.1,-100]}}}`)
		suite.assertQueryResult(list, "{root{"+valuesKey+"(at:1,count:2)}}", `{"data":{"root":{"`+valuesKey+`":[1.1,-100]}}}`)

		list = types.NewList(context.Background(), suite.vs, types.String("a"), types.String("b"), types.String("c"))
		suite.assertQueryResult(list, "{root{"+valuesKey+"(at:4)}}", `{"data":{"root":{"`+valuesKey+`":[]}}}`)
		suite.assertQueryResult(list, "{root{"+valuesKey+"(count:0)}}", `{"data":{"root":{"`+valuesKey+`":[]}}}`)
		suite.assertQueryResult(list, "{root{"+valuesKey+"(count:10)}}", `{"data":{"root":{"`+valuesKey+`":["a","b","c"]}}}`)
		suite.assertQueryResult(list, "{root{"+valuesKey+"(at:-1)}}", `{"data":{"root":{"`+valuesKey+`":["a","b","c"]}}}`)
	}
}

func (suite *QueryGraphQLSuite) TestListOfStruct() {
	list := types.NewList(context.Background(), suite.vs,
		types.NewStruct("Foo", types.StructData{
			"a": types.Float(28),
			"b": types.String("foo"),
		}),
		types.NewStruct("Foo", types.StructData{
			"a": types.Float(-20.102),
			"b": types.String("bar"),
		}),
		types.NewStruct("Foo", types.StructData{
			"a": types.Float(5),
			"b": types.String("baz"),
		}),
	)

	suite.assertQueryResult(list, "{root{elements{a b}}}", `{"data":{"root":{"elements":[{"a":28,"b":"foo"},{"a":-20.102,"b":"bar"},{"a":5,"b":"baz"}]}}}`)

	suite.assertQueryResult(list, "{root{elements{a}}}", `{"data":{"root":{"elements":[{"a":28},{"a":-20.102},{"a":5}]}}}`)
}

func (suite *QueryGraphQLSuite) TestListOfStructWithOptionalFields() {
	list := types.NewList(context.Background(), suite.vs,
		types.NewStruct("Foo", types.StructData{
			"a": types.Float(1),
		}),
		types.NewStruct("Foo", types.StructData{
			"a": types.Float(2),
			"b": types.String("bar"),
		}),
	)

	suite.assertQueryResult(list, "{root{elements{a b}}}", `{
                "data": {
                        "root": {
                                "elements": [
                                        {"a": 1, "b": null},
                                        {"a": 2, "b": "bar"}
                                ]
                        }
                }
        }`)
}

func (suite *QueryGraphQLSuite) TestSetBasic() {
	for _, valuesKey := range []string{"elements", "values"} {
		set := types.NewSet(context.Background(), suite.vs)
		suite.assertQueryResult(set, "{root{size}}", `{"data":{"root":{"size":0}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"}}", `{"data":{"root":{}}}`)

		set = types.NewSet(context.Background(), suite.vs, types.String("foo"), types.String("bar"), types.String("baz"))

		suite.assertQueryResult(set, "{root{"+valuesKey+"}}", `{"data":{"root":{"`+valuesKey+`":["bar","baz","foo"]}}}`)
		suite.assertQueryResult(set, "{root{size}}", `{"data":{"root":{"size":3}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"(count:2)}}", `{"data":{"root":{"`+valuesKey+`":["bar","baz"]}}}`)

		set = types.NewSet(context.Background(), suite.vs, types.Bool(true), types.Bool(false))

		suite.assertQueryResult(set, "{root{"+valuesKey+"}}", `{"data":{"root":{"`+valuesKey+`":[false,true]}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"(count:1)}}", `{"data":{"root":{"`+valuesKey+`":[false]}}}`)

		set = types.NewSet(context.Background(), suite.vs, types.Float(1), types.Float(1.1), types.Float(-100))

		suite.assertQueryResult(set, "{root{"+valuesKey+"}}", `{"data":{"root":{"`+valuesKey+`":[-100,1,1.1]}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"(count:2)}}", `{"data":{"root":{"`+valuesKey+`":[-100,1]}}}`)

		set = types.NewSet(context.Background(), suite.vs, types.String("a"), types.String("b"), types.String("c"), types.String("d"))
		suite.assertQueryResult(set, "{root{"+valuesKey+"(count:0)}}", `{"data":{"root":{"`+valuesKey+`":[]}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"(count:2)}}", `{"data":{"root":{"`+valuesKey+`":["a","b"]}}}`)

		suite.assertQueryResult(set, "{root{"+valuesKey+"(at:0,count:2)}}", `{"data":{"root":{"`+valuesKey+`":["a","b"]}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"(at:-1,count:2)}}", `{"data":{"root":{"`+valuesKey+`":["a","b"]}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"(at:1,count:2)}}", `{"data":{"root":{"`+valuesKey+`":["b","c"]}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"(at:2)}}", `{"data":{"root":{"`+valuesKey+`":["c","d"]}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"(at:2,count:1)}}", `{"data":{"root":{"`+valuesKey+`":["c"]}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"(at:2,count:0)}}", `{"data":{"root":{"`+valuesKey+`":[]}}}`)
		suite.assertQueryResult(set, "{root{"+valuesKey+"(at:2,count:10)}}", `{"data":{"root":{"`+valuesKey+`":["c","d"]}}}`)
	}
}

func (suite *QueryGraphQLSuite) TestSetOfStruct() {
	set := types.NewSet(context.Background(), suite.vs,
		types.NewStruct("Foo", types.StructData{
			"a": types.Float(28),
			"b": types.String("foo"),
		}),
		types.NewStruct("Foo", types.StructData{
			"a": types.Float(-20.102),
			"b": types.String("bar"),
		}),
		types.NewStruct("Foo", types.StructData{
			"a": types.Float(5),
			"b": types.String("baz"),
		}),
	)

	suite.assertQueryResult(set, "{root{values{a b}}}",
		`{"data":{"root":{"values":[{"a":28,"b":"foo"},{"a":5,"b":"baz"},{"a":-20.102,"b":"bar"}]}}}`)
	suite.assertQueryResult(set, "{root{values{a}}}", `{"data":{"root":{"values":[{"a":28},{"a":5},{"a":-20.102}]}}}`)
}

func (suite *QueryGraphQLSuite) TestMapBasic() {
	for _, entriesKey := range []string{"elements", "entries"} {

		m := types.NewMap(context.Background(), suite.vs)
		suite.assertQueryResult(m, "{root{size}}", `{"data":{"root":{"size":0}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"}}", `{"data":{"root":{}}}`)

		m = types.NewMap(context.Background(), suite.vs,
			types.String("a"), types.Float(1),
			types.String("b"), types.Float(2),
			types.String("c"), types.Float(3),
			types.String("d"), types.Float(4),
		)

		suite.assertQueryResult(m, "{root{"+entriesKey+"{key value}}}", `{"data":{"root":{"`+entriesKey+`":[{"key":"a","value":1},{"key":"b","value":2},{"key":"c","value":3},{"key":"d","value":4}]}}}`)
		suite.assertQueryResult(m, "{root{size}}", `{"data":{"root":{"size":4}}}`)
	}
}

func (suite *QueryGraphQLSuite) TestMapOfStruct() {
	m := types.NewMap(context.Background(), suite.vs,
		types.String("foo"), types.NewStruct("Foo", types.StructData{
			"a": types.Float(28),
			"b": types.String("foo"),
		}),
		types.String("bar"), types.NewStruct("Foo", types.StructData{
			"a": types.Float(-20.102),
			"b": types.String("bar"),
		}),
		types.String("baz"), types.NewStruct("Foo", types.StructData{
			"a": types.Float(5),
			"b": types.String("baz"),
		}),
	)

	suite.assertQueryResult(m, "{root{entries{key value{a}}}}", `{"data":{"root":{"entries":[{"key":"bar","value":{"a":-20.102}},{"key":"baz","value":{"a":5}},{"key":"foo","value":{"a":28}}]}}}`)
	suite.assertQueryResult(m, "{root{entries(count:1){value{a b}}}}", `{"data":{"root":{"entries":[{"value":{"a":-20.102,"b":"bar"}}]}}}`)
	suite.assertQueryResult(m, "{root{entries(count:3){key}}}", `{"data":{"root":{"entries":[{"key":"bar"},{"key":"baz"},{"key":"foo"}]}}}`)
}

func (suite *QueryGraphQLSuite) TestRef() {
	r := suite.vs.WriteValue(context.Background(), types.Float(100))

	suite.assertQueryResult(r, "{root{targetValue}}", `{"data":{"root":{"targetValue":100}}}`)
	suite.assertQueryResult(r, "{root{targetHash}}", `{"data":{"root":{"targetHash":"0123456789abcdefghijklmnopqrstuv"}}}`)
	suite.assertQueryResult(r, "{root{targetValue targetHash}}", `{"data":{"root":{"targetHash":"0123456789abcdefghijklmnopqrstuv","targetValue":100}}}`)

	r = suite.vs.WriteValue(context.Background(), types.NewStruct("Foo", types.StructData{
		"a": types.Float(28),
		"b": types.String("foo"),
	}))

	suite.assertQueryResult(r, "{root{targetValue{a}}}", `{"data":{"root":{"targetValue":{"a":28}}}}`)
	suite.assertQueryResult(r, "{root{targetValue{a b}}}", `{"data":{"root":{"targetValue":{"a":28,"b":"foo"}}}}`)

	r = suite.vs.WriteValue(context.Background(), types.NewList(context.Background(), suite.vs, types.String("foo"), types.String("bar"), types.String("baz")))

	suite.assertQueryResult(r, "{root{targetValue{values}}}", `{"data":{"root":{"targetValue":{"values":["foo","bar","baz"]}}}}`)
	suite.assertQueryResult(r, "{root{targetValue{values(at:1,count:2)}}}", `{"data":{"root":{"targetValue":{"values":["bar","baz"]}}}}`)
}

func (suite *QueryGraphQLSuite) TestListOfUnionOfStructs() {
	list := types.NewList(context.Background(), suite.vs,
		types.NewStruct("Foo", types.StructData{
			"a": types.Float(28),
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
		fmt.Sprintf("{root{values{... on %s{a b} ... on %s{b} ... on %s{c}}}}",
			GetTypeName(context.Background(), types.TypeOf(list.Get(context.Background(), 0))),
			GetTypeName(context.Background(), types.TypeOf(list.Get(context.Background(), 1))),
			GetTypeName(context.Background(), types.TypeOf(list.Get(context.Background(), 2)))),
		`{"data":{"root":{"values":[{"a":28,"b":"baz"},{"b":"bar"},{"c":true}]}}}`)
}

func (suite *QueryGraphQLSuite) TestListOfUnionOfStructsConflictingFieldTypes() {
	list := types.NewList(context.Background(), suite.vs,
		types.NewStruct("Foo", types.StructData{
			"a": types.Float(28),
		}),
		types.NewStruct("Bar", types.StructData{
			"a": types.String("bar"),
		}),
		types.NewStruct("Baz", types.StructData{
			"a": types.Bool(true),
		}),
	)

	suite.assertQueryResult(list,
		fmt.Sprintf("{root{values{... on %s{a} ... on %s{b: a} ... on %s{c: a}}}}",
			GetTypeName(context.Background(), types.TypeOf(list.Get(context.Background(), 0))),
			GetTypeName(context.Background(), types.TypeOf(list.Get(context.Background(), 1))),
			GetTypeName(context.Background(), types.TypeOf(list.Get(context.Background(), 2)))),
		`{"data":{"root":{"values":[{"a":28},{"b":"bar"},{"c":true}]}}}`)
}

func (suite *QueryGraphQLSuite) TestListOfUnionOfScalars() {
	list := types.NewList(context.Background(), suite.vs,
		types.Float(28),
		types.String("bar"),
		types.Bool(true),
	)

	suite.assertQueryResult(list, "{root{values{... on BooleanValue{b: scalarValue} ... on StringValue{s: scalarValue} ... on FloatValue{n: scalarValue}}}}", `{"data":{"root":{"values":[{"n":28},{"s":"bar"},{"b":true}]}}}`)
}

func (suite *QueryGraphQLSuite) TestCyclicStructs() {
	// struct A {
	//  a: "aaa"
	//  b: Set(struct A {
	// 	 a: "bbb"
	// 	 b: Set()
	//  })
	// }

	s1 := types.NewStruct("A", types.StructData{
		"a": types.String("aaa"),
		"b": types.NewSet(context.Background(), suite.vs,
			types.NewStruct("A", types.StructData{
				"a": types.String("bbb"),
				"b": types.NewSet(context.Background(), suite.vs),
			})),
	})

	suite.assertQueryResult(s1, "{root{a b{values{a}}}}", `{"data":{"root":{"a":"aaa","b":{"values":[{"a":"bbb"}]}}}}`)
}

func (suite *QueryGraphQLSuite) TestCyclicStructsWithUnion() {
	// struct A {
	//  a: "aaa"
	//  b: Struct A {
	// 	 a: "bbb"
	// 	 b: 42
	//  })
	// }

	// struct A {
	//   a: String,
	//   b: Float | Cycle<A>,
	// }

	s1 := types.NewStruct("A", types.StructData{
		"a": types.String("aaa"),
		"b": types.NewStruct("A", types.StructData{
			"a": types.String("bbb"),
			"b": types.Float(42),
		}),
	})

	suite.assertQueryResult(s1,
		`{
                        root{
                                a
                                b {
                                        a
                                        b {
                                                scalarValue
                                        }
                                }
                        }
                }
                `,
		`{
                        "data": {
                                "root": {
                                        "a": "aaa",
                                        "b": {
                                                "a": "bbb",
                                                "b": {
                                                        "scalarValue": 42
                                                }
                                        }
                                }
                        }
                }`)

	suite.assertQueryResult(s1,
		fmt.Sprintf(`{
	                root{
	                        a
	                        b {
	                                ... on %s {
	                                        a
	                                }
	                        }
	                }
	        }`, GetTypeName(context.Background(), types.TypeOf(s1))),
		`{
	                "data": {
	                        "root": {
	                                "a": "aaa",
	                                "b": {
	                                        "a": "bbb"
	                                }
	                        }
	                }
	        }`)
}

func (suite *QueryGraphQLSuite) TestNestedCollection() {
	list := types.NewList(context.Background(), suite.vs,
		types.NewSet(context.Background(), suite.vs,
			types.NewMap(context.Background(), suite.vs, types.Float(10), types.String("foo")),
			types.NewMap(context.Background(), suite.vs, types.Float(20), types.String("bar")),
		),
		types.NewSet(context.Background(), suite.vs,
			types.NewMap(context.Background(), suite.vs, types.Float(30), types.String("baz")),
			types.NewMap(context.Background(), suite.vs, types.Float(40), types.String("bat")),
		),
	)

	suite.assertQueryResult(list, "{root{size}}", `{"data":{"root":{"size":2}}}`)
	suite.assertQueryResult(list, "{root{values(count:1){size}}}", `{"data":{"root":{"values":[{"size":2}]}}}`)
	suite.assertQueryResult(list, "{root{values(at:1,count:1){values(count:1){entries{key value}}}}}",
		`{"data":{"root":{"values":[{"values":[{"entries":[{"key":40,"value":"bat"}]}]}]}}}`)
}

func (suite *QueryGraphQLSuite) TestLoFi() {
	b := types.NewBlob(context.Background(), suite.vs, bytes.NewBufferString("I am a blob"))

	suite.assertQueryResult(b, "{root}", `{"data":{"root":"0123456789abcdefghijklmnopqrstuv"}}`)

	t := types.StringType
	suite.assertQueryResult(t, "{root}", `{"data":{"root":"0123456789abcdefghijklmnopqrstuv"}}`)
}

func (suite *QueryGraphQLSuite) TestError() {
	buff := &bytes.Buffer{}
	Error(errors.New("Some error string"), buff)
	suite.Equal(buff.String(), `{"data":null,"errors":[{"message":"Some error string","locations":null}]}
`)
}

func (suite *QueryGraphQLSuite) TestMapArgs() {
	for _, entriesKey := range []string{"elements", "entries"} {

		m := types.NewMap(context.Background(), suite.vs,
			types.String("a"), types.Float(1),
			types.String("c"), types.Float(2),
			types.String("e"), types.Float(3),
			types.String("g"), types.Float(4),
		)

		// count
		suite.assertQueryResult(m, "{root{"+entriesKey+"(count:0){value}}}", `{"data":{"root":{"`+entriesKey+`":[]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(count:2){value}}}", `{"data":{"root":{"`+entriesKey+`":[{"value":1},{"value":2}]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(count:3){key}}}", `{"data":{"root":{"`+entriesKey+`":[{"key":"a"},{"key":"c"},{"key":"e"}]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(count: -1){key}}}", `{"data":{"root":{"`+entriesKey+`":[]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(count:5){value}}}", `{"data":{"root":{"`+entriesKey+`":[{"value":1},{"value":2},{"value":3},{"value":4}]}}}`)

		// at
		suite.assertQueryResult(m, "{root{"+entriesKey+"(at:0){value}}}", `{"data":{"root":{"`+entriesKey+`":[{"value":1},{"value":2},{"value":3},{"value":4}]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(at:-1){value}}}", `{"data":{"root":{"`+entriesKey+`":[{"value":1},{"value":2},{"value":3},{"value":4}]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(at:2){value}}}", `{"data":{"root":{"`+entriesKey+`":[{"value":3},{"value":4}]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(at:5){value}}}", `{"data":{"root":{"`+entriesKey+`":[]}}}`)

		// at & count
		suite.assertQueryResult(m, "{root{"+entriesKey+"(at:0,count:2){value}}}", `{"data":{"root":{"`+entriesKey+`":[{"value":1},{"value":2}]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(at:-1,count:2){value}}}", `{"data":{"root":{"`+entriesKey+`":[{"value":1},{"value":2}]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(at:1,count:2){value}}}", `{"data":{"root":{"`+entriesKey+`":[{"value":2},{"value":3}]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(at:2,count:1){value}}}", `{"data":{"root":{"`+entriesKey+`":[{"value":3}]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(at:2,count:0){value}}}", `{"data":{"root":{"`+entriesKey+`":[]}}}`)
		suite.assertQueryResult(m, "{root{"+entriesKey+"(at:2,count:10){value}}}", `{"data":{"root":{"`+entriesKey+`":[{"value":3},{"value":4}]}}}`)

		// key
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"e"){key value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"e","value":3}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"g"){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"value":4}]}}}`)
		// "f", no count/through so asking for exact match
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"f"){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[]}}}`)
		// "x" is larger than end
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"x"){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[]}}}`)

		// key & at
		// at is ignored when key is present
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"e",at:2){key value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"e","value":3}]}}}`)

		// key & count
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"c", count: 2){key value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"c","value":2},{"key":"e","value":3}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"c", count: 0){key value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"c", count: -1){key value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"e", count: 5){key value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"e","value":3},{"key":"g","value":4}]}}}`)

		// through
		suite.assertQueryResult(m, `{root{`+entriesKey+`(through:"c"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"a"},{"key":"c"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(through:"b"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"a"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(through:"0"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[]}}}`)

		// key & through
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"c", through:"c"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"c"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"c",through:"e"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"c"},{"key":"e"}]}}}`)

		// through & count
		suite.assertQueryResult(m, `{root{`+entriesKey+`(through:"c",count:1){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"a"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(through:"b",count:0){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(through:"0",count:10){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[]}}}`)

		// at & through
		suite.assertQueryResult(m, `{root{`+entriesKey+`(at:0,through:"a"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"a"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(at:1,through:"e"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"c"},{"key":"e"}]}}}`)

		// at & count & through
		suite.assertQueryResult(m, `{root{`+entriesKey+`(at:0,count:2,through:"a"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"a"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(at:0,count:2,through:"e"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"a"},{"key":"c"}]}}}`)

		// key & count & through
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"c",count:2,through:"c"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"c"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(key:"c",count:2,through:"g"){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"c"},{"key":"e"}]}}}`)
	}
}

func (suite *QueryGraphQLSuite) TestMapKeysArg() {
	for _, entriesKey := range []string{"elements", "entries"} {
		m := types.NewMap(context.Background(), suite.vs,
			types.String("a"), types.Float(1),
			types.String("c"), types.Float(2),
			types.String("e"), types.Float(3),
			types.String("g"), types.Float(4),
		)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(keys:["c","a"]){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"value":2},{"value":1}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(keys:[]){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[]}}}`)

		m = types.NewMap(context.Background(), suite.vs,
			types.Float(1), types.String("a"),
			types.Float(2), types.String("c"),
			types.Float(3), types.String("e"),
			types.Float(4), types.String("g"),
		)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(keys:[4,1]){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"value":"g"},{"value":"a"}]}}}`)

		// Ignore other args
		suite.assertQueryResult(m, `{root{`+entriesKey+`(keys:[4,1],key:2){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"value":"g"},{"value":"a"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(keys:[4,1],count:0){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"value":"g"},{"value":"a"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(keys:[4,1],at:4){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"value":"g"},{"value":"a"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(keys:[4,1],through:1){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"value":"g"},{"value":"a"}]}}}`)
	}
}

func (suite *QueryGraphQLSuite) TestSetArgs() {
	for _, valuesKey := range []string{"elements", "values"} {
		s := types.NewSet(context.Background(), suite.vs,
			types.String("a"),
			types.String("c"),
			types.String("e"),
			types.String("g"),
		)

		// count
		suite.assertQueryResult(s, "{root{"+valuesKey+"(count:0)}}", `{"data":{"root":{"`+valuesKey+`":[]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(count:2)}}", `{"data":{"root":{"`+valuesKey+`":["a","c"]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(count:3)}}", `{"data":{"root":{"`+valuesKey+`":["a","c","e"]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(count: -1)}}", `{"data":{"root":{"`+valuesKey+`":[]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(count:5)}}", `{"data":{"root":{"`+valuesKey+`":["a","c","e","g"]}}}`)

		// at
		suite.assertQueryResult(s, "{root{"+valuesKey+"(at:0)}}", `{"data":{"root":{"`+valuesKey+`":["a","c","e","g"]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(at:-1)}}", `{"data":{"root":{"`+valuesKey+`":["a","c","e","g"]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(at:2)}}", `{"data":{"root":{"`+valuesKey+`":["e","g"]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(at:5)}}", `{"data":{"root":{"`+valuesKey+`":[]}}}`)

		// at & count
		suite.assertQueryResult(s, "{root{"+valuesKey+"(at:0,count:2)}}", `{"data":{"root":{"`+valuesKey+`":["a","c"]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(at:-1,count:2)}}", `{"data":{"root":{"`+valuesKey+`":["a","c"]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(at:1,count:2)}}", `{"data":{"root":{"`+valuesKey+`":["c","e"]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(at:2,count:1)}}", `{"data":{"root":{"`+valuesKey+`":["e"]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(at:2,count:0)}}", `{"data":{"root":{"`+valuesKey+`":[]}}}`)
		suite.assertQueryResult(s, "{root{"+valuesKey+"(at:2,count:10)}}", `{"data":{"root":{"`+valuesKey+`":["e","g"]}}}`)

		// key
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"e")}}`,
			`{"data":{"root":{"`+valuesKey+`":["e"]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"g")}}`,
			`{"data":{"root":{"`+valuesKey+`":["g"]}}}`)
		// "f", no count/through so asking for exact match
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"f")}}`,
			`{"data":{"root":{"`+valuesKey+`":[]}}}`)
		// "x" is larger than end
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"x")}}`,
			`{"data":{"root":{"`+valuesKey+`":[]}}}`)
		// exact match
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"0")}}`,
			`{"data":{"root":{"`+valuesKey+`":[]}}}`)

		// key & at
		// at is ignored when key is present
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"e",at:2)}}`,
			`{"data":{"root":{"`+valuesKey+`":["e"]}}}`)

		// key & count
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"c", count: 2)}}`,
			`{"data":{"root":{"`+valuesKey+`":["c","e"]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"c", count: 0)}}`,
			`{"data":{"root":{"`+valuesKey+`":[]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"c", count: -1)}}`,
			`{"data":{"root":{"`+valuesKey+`":[]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"e", count: 5)}}`,
			`{"data":{"root":{"`+valuesKey+`":["e","g"]}}}`)

		// through
		suite.assertQueryResult(s, `{root{`+valuesKey+`(through:"c")}}`,
			`{"data":{"root":{"`+valuesKey+`":["a","c"]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(through:"b")}}`,
			`{"data":{"root":{"`+valuesKey+`":["a"]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(through:"0")}}`,
			`{"data":{"root":{"`+valuesKey+`":[]}}}`)

		// key & through
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"c", through:"c")}}`,
			`{"data":{"root":{"`+valuesKey+`":["c"]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"c",through:"e")}}`,
			`{"data":{"root":{"`+valuesKey+`":["c","e"]}}}`)

		// through & count
		suite.assertQueryResult(s, `{root{`+valuesKey+`(through:"c",count:1)}}`,
			`{"data":{"root":{"`+valuesKey+`":["a"]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(through:"b",count:0)}}`,
			`{"data":{"root":{"`+valuesKey+`":[]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(through:"0",count:10)}}`,
			`{"data":{"root":{"`+valuesKey+`":[]}}}`)

		// at & through
		suite.assertQueryResult(s, `{root{`+valuesKey+`(at:0,through:"a")}}`,
			`{"data":{"root":{"`+valuesKey+`":["a"]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(at:1,through:"e")}}`,
			`{"data":{"root":{"`+valuesKey+`":["c","e"]}}}`)

		// at & count & through
		suite.assertQueryResult(s, `{root{`+valuesKey+`(at:0,count:2,through:"a")}}`,
			`{"data":{"root":{"`+valuesKey+`":["a"]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(at:0,count:2,through:"e")}}`,
			`{"data":{"root":{"`+valuesKey+`":["a","c"]}}}`)

		// key & count & through
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"c",count:2,through:"c")}}`,
			`{"data":{"root":{"`+valuesKey+`":["c"]}}}`)
		suite.assertQueryResult(s, `{root{`+valuesKey+`(key:"c",count:2,through:"g")}}`,
			`{"data":{"root":{"`+valuesKey+`":["c","e"]}}}`)
	}
}

func (suite *QueryGraphQLSuite) TestMapValues() {
	m := types.NewMap(context.Background(), suite.vs,
		types.String("a"), types.Float(1),
		types.String("c"), types.Float(2),
		types.String("e"), types.Float(3),
		types.String("g"), types.Float(4),
	)

	suite.assertQueryResult(m, "{root{values}}", `{"data":{"root":{"values":[1,2,3,4]}}}`)

	// count
	suite.assertQueryResult(m, "{root{values(count:0)}}", `{"data":{"root":{"values":[]}}}`)
	suite.assertQueryResult(m, "{root{values(count:2)}}", `{"data":{"root":{"values":[1,2]}}}`)
	suite.assertQueryResult(m, "{root{values(count:3)}}", `{"data":{"root":{"values":[1,2,3]}}}`)
	suite.assertQueryResult(m, "{root{values(count: -1)}}", `{"data":{"root":{"values":[]}}}`)
	suite.assertQueryResult(m, "{root{values(count:5)}}", `{"data":{"root":{"values":[1,2,3,4]}}}`)

	// at
	suite.assertQueryResult(m, "{root{values(at:0)}}", `{"data":{"root":{"values":[1,2,3,4]}}}`)
	suite.assertQueryResult(m, "{root{values(at:-1)}}", `{"data":{"root":{"values":[1,2,3,4]}}}`)
	suite.assertQueryResult(m, "{root{values(at:2)}}", `{"data":{"root":{"values":[3,4]}}}`)
	suite.assertQueryResult(m, "{root{values(at:5)}}", `{"data":{"root":{"values":[]}}}`)

	// at & count
	suite.assertQueryResult(m, "{root{values(at:0,count:2)}}", `{"data":{"root":{"values":[1,2]}}}`)
	suite.assertQueryResult(m, "{root{values(at:-1,count:2)}}", `{"data":{"root":{"values":[1,2]}}}`)
	suite.assertQueryResult(m, "{root{values(at:1,count:2)}}", `{"data":{"root":{"values":[2,3]}}}`)
	suite.assertQueryResult(m, "{root{values(at:2,count:1)}}", `{"data":{"root":{"values":[3]}}}`)
	suite.assertQueryResult(m, "{root{values(at:2,count:0)}}", `{"data":{"root":{"values":[]}}}`)
	suite.assertQueryResult(m, "{root{values(at:2,count:10)}}", `{"data":{"root":{"values":[3,4]}}}`)

	// key
	suite.assertQueryResult(m, `{root{values(key:"e")}}`, `{"data":{"root":{"values":[3]}}}`)
	suite.assertQueryResult(m, `{root{values(key:"g")}}`, `{"data":{"root":{"values":[4]}}}`)
	// "f", no count/through so asking for exact match
	suite.assertQueryResult(m, `{root{values(key:"f")}}`, `{"data":{"root":{"values":[]}}}`)
	// "x" is larger than end
	suite.assertQueryResult(m, `{root{values(key:"x")}}`, `{"data":{"root":{"values":[]}}}`)

	// key & at
	// at is ignored when key is present
	suite.assertQueryResult(m, `{root{values(key:"e",at:2)}}`, `{"data":{"root":{"values":[3]}}}`)

	// key & count
	suite.assertQueryResult(m, `{root{values(key:"c",count:2)}}`, `{"data":{"root":{"values":[2,3]}}}`)
	suite.assertQueryResult(m, `{root{values(key:"c",count:0)}}`, `{"data":{"root":{"values":[]}}}`)
	suite.assertQueryResult(m, `{root{values(key:"c",count:-1)}}`, `{"data":{"root":{"values":[]}}}`)
	suite.assertQueryResult(m, `{root{values(key:"e",count:5)}}`, `{"data":{"root":{"values":[3,4]}}}`)

	// through
	suite.assertQueryResult(m, `{root{values(through:"c")}}`, `{"data":{"root":{"values":[1,2]}}}`)
	suite.assertQueryResult(m, `{root{values(through:"b")}}`, `{"data":{"root":{"values":[1]}}}`)
	suite.assertQueryResult(m, `{root{values(through:"0")}}`, `{"data":{"root":{"values":[]}}}`)

	// key & through
	suite.assertQueryResult(m, `{root{values(key:"c", through:"c")}}`, `{"data":{"root":{"values":[2]}}}`)
	suite.assertQueryResult(m, `{root{values(key:"c",through:"e")}}`, `{"data":{"root":{"values":[2,3]}}}`)

	// through & count
	suite.assertQueryResult(m, `{root{values(through:"c",count:1)}}`, `{"data":{"root":{"values":[1]}}}`)
	suite.assertQueryResult(m, `{root{values(through:"b",count:0)}}`, `{"data":{"root":{"values":[]}}}`)
	suite.assertQueryResult(m, `{root{values(through:"0",count:10)}}`, `{"data":{"root":{"values":[]}}}`)

	// at & through
	suite.assertQueryResult(m, `{root{values(at:0,through:"a")}}`, `{"data":{"root":{"values":[1]}}}`)
	suite.assertQueryResult(m, `{root{values(at:1,through:"e")}}`, `{"data":{"root":{"values":[2,3]}}}`)

	// at & count & through
	suite.assertQueryResult(m, `{root{values(at:0,count:2,through:"a")}}`, `{"data":{"root":{"values":[1]}}}`)
	suite.assertQueryResult(m, `{root{values(at:0,count:2,through:"e")}}`, `{"data":{"root":{"values":[1,2]}}}`)

	// key & count & through
	suite.assertQueryResult(m, `{root{values(key:"c",count:2,through:"c")}}`,
		`{"data":{"root":{"values":[2]}}}`)
	suite.assertQueryResult(m, `{root{values(key:"c",count:2,through:"g")}}`,
		`{"data":{"root":{"values":[2,3]}}}`)
}

func (suite *QueryGraphQLSuite) TestMapKeys() {
	m := types.NewMap(context.Background(), suite.vs,
		types.String("a"), types.Float(1),
		types.String("c"), types.Float(2),
		types.String("e"), types.Float(3),
		types.String("g"), types.Float(4),
	)

	suite.assertQueryResult(m, "{root{keys}}", `{"data":{"root":{"keys":["a","c","e","g"]}}}`)

	// count
	suite.assertQueryResult(m, "{root{keys(count:0)}}", `{"data":{"root":{"keys":[]}}}`)
	suite.assertQueryResult(m, "{root{keys(count:2)}}", `{"data":{"root":{"keys":["a","c"]}}}`)
	suite.assertQueryResult(m, "{root{keys(count:3)}}", `{"data":{"root":{"keys":["a","c","e"]}}}`)
	suite.assertQueryResult(m, "{root{keys(count: -1)}}", `{"data":{"root":{"keys":[]}}}`)
	suite.assertQueryResult(m, "{root{keys(count:5)}}", `{"data":{"root":{"keys":["a","c","e","g"]}}}`)

	// at
	suite.assertQueryResult(m, "{root{keys(at:0)}}", `{"data":{"root":{"keys":["a","c","e","g"]}}}`)
	suite.assertQueryResult(m, "{root{keys(at:-1)}}", `{"data":{"root":{"keys":["a","c","e","g"]}}}`)
	suite.assertQueryResult(m, "{root{keys(at:2)}}", `{"data":{"root":{"keys":["e","g"]}}}`)
	suite.assertQueryResult(m, "{root{keys(at:5)}}", `{"data":{"root":{"keys":[]}}}`)

	// at & count
	suite.assertQueryResult(m, "{root{keys(at:0,count:2)}}", `{"data":{"root":{"keys":["a","c"]}}}`)
	suite.assertQueryResult(m, "{root{keys(at:-1,count:2)}}", `{"data":{"root":{"keys":["a","c"]}}}`)
	suite.assertQueryResult(m, "{root{keys(at:1,count:2)}}", `{"data":{"root":{"keys":["c","e"]}}}`)
	suite.assertQueryResult(m, "{root{keys(at:2,count:1)}}", `{"data":{"root":{"keys":["e"]}}}`)
	suite.assertQueryResult(m, "{root{keys(at:2,count:0)}}", `{"data":{"root":{"keys":[]}}}`)
	suite.assertQueryResult(m, "{root{keys(at:2,count:10)}}", `{"data":{"root":{"keys":["e","g"]}}}`)

	// key
	suite.assertQueryResult(m, `{root{keys(key:"e")}}`, `{"data":{"root":{"keys":["e"]}}}`)
	suite.assertQueryResult(m, `{root{keys(key:"g")}}`, `{"data":{"root":{"keys":["g"]}}}`)
	// "f", no count/through so asking for exact match
	suite.assertQueryResult(m, `{root{keys(key:"f")}}`, `{"data":{"root":{"keys":[]}}}`)
	// "x" is larger than end
	suite.assertQueryResult(m, `{root{keys(key:"x")}}`, `{"data":{"root":{"keys":[]}}}`)

	// key & at
	// at is ignored when key is present
	suite.assertQueryResult(m, `{root{keys(key:"e",at:2)}}`, `{"data":{"root":{"keys":["e"]}}}`)

	// key & count
	suite.assertQueryResult(m, `{root{keys(key:"c",count:2)}}`, `{"data":{"root":{"keys":["c","e"]}}}`)
	suite.assertQueryResult(m, `{root{keys(key:"c",count:0)}}`, `{"data":{"root":{"keys":[]}}}`)
	suite.assertQueryResult(m, `{root{keys(key:"c",count:-1)}}`, `{"data":{"root":{"keys":[]}}}`)
	suite.assertQueryResult(m, `{root{keys(key:"e",count:5)}}`, `{"data":{"root":{"keys":["e","g"]}}}`)

	// through
	suite.assertQueryResult(m, `{root{keys(through:"c")}}`, `{"data":{"root":{"keys":["a","c"]}}}`)
	suite.assertQueryResult(m, `{root{keys(through:"b")}}`, `{"data":{"root":{"keys":["a"]}}}`)
	suite.assertQueryResult(m, `{root{keys(through:"0")}}`, `{"data":{"root":{"keys":[]}}}`)

	// key & through
	suite.assertQueryResult(m, `{root{keys(key:"c", through:"c")}}`, `{"data":{"root":{"keys":["c"]}}}`)
	suite.assertQueryResult(m, `{root{keys(key:"c",through:"e")}}`, `{"data":{"root":{"keys":["c","e"]}}}`)

	// through & count
	suite.assertQueryResult(m, `{root{keys(through:"c",count:1)}}`, `{"data":{"root":{"keys":["a"]}}}`)
	suite.assertQueryResult(m, `{root{keys(through:"b",count:0)}}`, `{"data":{"root":{"keys":[]}}}`)
	suite.assertQueryResult(m, `{root{keys(through:"0",count:10)}}`, `{"data":{"root":{"keys":[]}}}`)

	// at & through
	suite.assertQueryResult(m, `{root{keys(at:0,through:"a")}}`, `{"data":{"root":{"keys":["a"]}}}`)
	suite.assertQueryResult(m, `{root{keys(at:1,through:"e")}}`, `{"data":{"root":{"keys":["c","e"]}}}`)

	// at & count & through
	suite.assertQueryResult(m, `{root{keys(at:0,count:2,through:"a")}}`, `{"data":{"root":{"keys":["a"]}}}`)
	suite.assertQueryResult(m, `{root{keys(at:0,count:2,through:"e")}}`, `{"data":{"root":{"keys":["a","c"]}}}`)

	// key & count & through
	suite.assertQueryResult(m, `{root{keys(key:"c",count:2,through:"c")}}`,
		`{"data":{"root":{"keys":["c"]}}}`)
	suite.assertQueryResult(m, `{root{keys(key:"c",count:2,through:"g")}}`,
		`{"data":{"root":{"keys":["c","e"]}}}`)
}

func (suite *QueryGraphQLSuite) TestMapNullable() {
	// When selecting the result based on keys the values may be null.
	m := types.NewMap(context.Background(), suite.vs,
		types.String("a"), types.Float(1),
		types.String("c"), types.Float(2),
	)

	for _, entriesKey := range []string{"elements", "entries"} {
		suite.assertQueryResult(m, `{root{`+entriesKey+`(keys:["a","b","c"]){value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"value":1},{"value":null},{"value":2}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(keys:["a","b","c"]){key}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"a"},{"key":"b"},{"key":"c"}]}}}`)
		suite.assertQueryResult(m, `{root{`+entriesKey+`(keys:["a","b","c"]){key value}}}`,
			`{"data":{"root":{"`+entriesKey+`":[{"key":"a","value":1},{"key":"b","value":null},{"key":"c","value":2}]}}}`)
	}
	suite.assertQueryResult(m, `{root{values(keys:["a","b","c"])}}`,
		`{"data":{"root":{"values":[1,null,2]}}}`)
	suite.assertQueryResult(m, `{root{keys(keys:["a","b","c"])}}`,
		`{"data":{"root":{"keys":["a","b","c"]}}}`)
}

func (suite *QueryGraphQLSuite) TestStructWithOptionalField() {
	tm := NewTypeMap()
	rootValue := types.NewStruct("", types.StructData{
		"n": types.Float(42),
	})
	rootType := NomsTypeToGraphQLType(context.Background(), types.MakeStructType("",
		types.StructField{Name: "n", Type: types.FloaTType, Optional: false},
		types.StructField{Name: "s", Type: types.StringType, Optional: true},
	), false, tm)

	queryObj := graphql.NewObject(graphql.ObjectConfig{
		Name: rootQueryKey,
		Fields: graphql.Fields{
			rootKey: &graphql.Field{
				Type: rootType,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return MaybeGetScalar(rootValue), nil
				},
			},
		}})

	schemaConfig := graphql.SchemaConfig{Query: queryObj}
	schema, err := graphql.NewSchema(schemaConfig)
	suite.NoError(err)
	ctx := NewContext(context.Background(), suite.vs)
	query := `{root{n s}}`

	r := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		Context:       ctx,
	})

	suite.Equal(map[string]interface{}{"root": map[string]interface{}{"n": float64(42), "s": nil}}, r.Data)
}

func (suite *QueryGraphQLSuite) TestMutationScalarArgs() {
	test := func(query, expected string, nomsType *types.Type) {
		tc := NewTypeConverter()
		inType, err := tc.NomsTypeToGraphQLInputType(context.Background(), nomsType)
		suite.NoError(err)
		outType := tc.NomsTypeToGraphQLType(context.Background(), nomsType)
		suite.assertMutationTypes(query, expected, tc, inType, outType, func(p graphql.ResolveParams) (interface{}, error) {
			return p.Args["new"], nil
		})
	}

	test(`mutation {test(new: 123)}`, `{"data": {"test": 123}}`, types.FloaTType)
	test(`mutation {test(new: 0)}`, `{"data": {"test": 0}}`, types.FloaTType)

	test(`mutation {test(new: "hi")}`, `{"data": {"test": "hi"}}`, types.StringType)
	test(`mutation {test(new: "")}`, `{"data": {"test": ""}}`, types.StringType)

	test(`mutation {test(new: true)}`, `{"data": {"test": true}}`, types.BoolType)
	test(`mutation {test(new: false)}`, `{"data": {"test": false}}`, types.BoolType)
}

func (suite *QueryGraphQLSuite) TestMutationWeirdosArgs() {
	test := func(query, expected string, nomsType *types.Type) {
		tc := NewTypeConverter()
		inType, err := tc.NomsTypeToGraphQLInputType(context.Background(), nomsType)
		suite.NoError(err)
		outType := graphql.String
		suite.assertMutationTypes(query, expected, tc, inType, outType, func(p graphql.ResolveParams) (interface{}, error) {
			return p.Args["new"], nil
		})
	}

	test(`mutation {test(new: "#abc")}`, `{"data": {"test": "#abc"}}`, types.MakeRefType(types.FloaTType))
	test(`mutation {test(new: "0123456789")}`, `{"data": {"test": "0123456789"}}`, types.BlobType)
}

func (suite *QueryGraphQLSuite) assertMutationTypes(query, expected string, tc *TypeConverter, inType graphql.Input, outType graphql.Type, resolver graphql.FieldResolveFn) {
	buf := &bytes.Buffer{}
	root := types.Float(0)
	schemaConfig := graphql.SchemaConfig{
		Mutation: graphql.NewObject(graphql.ObjectConfig{
			Name: "Mutation",
			Fields: graphql.Fields{
				"test": &graphql.Field{
					Type: outType,
					Args: graphql.FieldConfigArgument{
						"new": &graphql.ArgumentConfig{
							Type: inType,
						},
					},
					Resolve: resolver,
				},
			},
		}),
	}
	queryWithSchemaConfig(context.Background(), root, query, schemaConfig, suite.vs, tc, buf)
	suite.JSONEq(expected, buf.String())
}

func (suite *QueryGraphQLSuite) TestMutationCollectionArgs() {
	test := func(query, expected string, expectedArg interface{}, nomsType *types.Type) {
		tc := NewTypeConverter()
		inType, err := tc.NomsTypeToGraphQLInputType(context.Background(), nomsType)
		suite.NoError(err)
		outType := graphql.Boolean
		suite.assertMutationTypes(query, expected, tc, inType, outType, func(p graphql.ResolveParams) (interface{}, error) {
			suite.Equal(expectedArg, p.Args["new"])
			return true, nil
		})
	}

	test(`mutation {test(new: [0, 1, 2, 3])}`, `{"data": {"test": true}}`, []interface{}{float64(0), float64(1), float64(2), float64(3)}, types.MakeListType(types.FloaTType))
	test(`mutation {test(new: [])}`, `{"data": {"test": true}}`, []interface{}{}, types.MakeListType(types.FloaTType))

	test(`mutation {test(new: [0, 1, 2, 3])}`, `{"data": {"test": true}}`, []interface{}{float64(0), float64(1), float64(2), float64(3)}, types.MakeSetType(types.FloaTType))
	test(`mutation {test(new: [])}`, `{"data": {"test": true}}`, []interface{}{}, types.MakeSetType(types.FloaTType))

	test(`mutation {
                test(new: [
                        {
                                key: 1,
                                value: "a"
                        }, {
                                key: 2,
                                value: "b"
                        }
                ])
        }`, `{"data": {"test": true}}`, []interface{}{
		map[string]interface{}{"key": float64(1), "value": "a"},
		map[string]interface{}{"key": float64(2), "value": "b"},
	}, types.MakeMapType(types.FloaTType, types.StringType))
	test(`mutation {test(new: [])}`, `{"data": {"test": true}}`, []interface{}{}, types.MakeMapType(types.FloaTType, types.StringType))

	st := types.MakeStructTypeFromFields("N", types.FieldMap{
		"f": types.FloaTType,
		"b": types.BoolType,
		"s": types.StringType,
	})
	test(`mutation {test(new: {
                f: 42,
                b: true,
                s: "hi"
        })}`, `{"data": {"test": true}}`, map[string]interface{}{"b": true, "f": float64(42), "s": "hi"}, st)
}

func (suite *QueryGraphQLSuite) TestMapWithComplexKeys() {
	m := types.NewMap(context.Background(), suite.vs,
		types.NewList(context.Background(), suite.vs, types.String("a")), types.Float(1),
		types.NewList(context.Background(), suite.vs, types.String("c")), types.Float(2),
		types.NewList(context.Background(), suite.vs, types.String("e")), types.Float(3),
		types.NewList(context.Background(), suite.vs, types.String("g")), types.Float(4),
	)

	suite.assertQueryResult(m, `{root{values(key: ["e"])}}`, `{"data":{"root":{"values":[3]}}}`)
	suite.assertQueryResult(m, `{root{values(key: [])}}`, `{"data":{"root":{"values":[]}}}`)

	// The ordering here depends on the hash of the value...
	suite.assertQueryResult(m, `{root{values(key: ["a"], through: ["e"])}}`, `{"data":{"root":{"values":[1, 2, 3]}}}`)

	suite.assertQueryResult(m, `{root{values(keys: [["a"],["b"],["c"]])}}`, `{"data":{"root":{"values":[1, null, 2]}}}`)
	suite.assertQueryResult(m, `{
                root {
                        keys(keys: [["a"],["b"],["c"]]) {
                                values
                        }
                }
        }`, `{"data": {
                "root": {
                        "keys": [
                                {"values": ["a"]},
                                {"values": ["b"]},
                                {"values": ["c"]}
                        ]
                }
        }}`)

	m2 := types.NewMap(context.Background(), suite.vs,
		types.NewStruct("", types.StructData{
			"n": types.String("a"),
		}), types.Float(1),
		types.NewStruct("", types.StructData{
			"n": types.String("c"),
		}), types.Float(2),
		types.NewStruct("", types.StructData{
			"n": types.String("e"),
		}), types.Float(3),
		types.NewStruct("", types.StructData{
			"n": types.String("g"),
		}), types.Float(4),
	)
	suite.assertQueryResult(m2, `{root{values(key: {n: "e"})}}`, `{"data":{"root":{"values":[3]}}}`)
	suite.assertQueryResult(m2, `{root{values(key: {n: "x"})}}`, `{"data":{"root":{"values":[]}}}`)
	// The order is based on hash
	suite.assertQueryResult(m2, `{root{values(key: {n: "g"}, through: {n: "c"})}}`, `{"data":{"root":{"values":[4,2]}}}`)
	suite.assertQueryResult(m2, `{root{values(keys: [{n: "a"}, {n: "b"}, {n: "c"}])}}`,
		`{"data":{"root":{"values":[1, null, 2]}}}`)
	suite.assertQueryResult(m2, `{root{keys(keys: [{n: "a"}, {n: "b"}, {n: "c"}]) { n }}}`,
		`{"data":{"root":{"keys":[{"n": "a"}, {"n": "b"}, {"n": "c"}]}}}`)
}

func (suite *QueryGraphQLSuite) TestSetWithComplexKeys() {
	s := types.NewSet(context.Background(), suite.vs,
		types.NewList(context.Background(), suite.vs, types.String("a")),
		types.NewList(context.Background(), suite.vs, types.String("c")),
		types.NewList(context.Background(), suite.vs, types.String("e")),
		types.NewList(context.Background(), suite.vs, types.String("g")),
	)

	suite.assertQueryResult(s, `{root{values(key: ["e"]) { values }}}`,
		`{"data":{"root":{"values":[{"values":["e"]}]}}}`)
	suite.assertQueryResult(s, `{root{values(key: []) { values }}}`, `{"data":{"root":{"values":[]}}}`)

	// The ordering here depends on the hash of the value...
	suite.assertQueryResult(s, `{root{values(key: ["g"], through: ["c"]) { values }}}`,
		`{"data":{"root":{"values":[{"values":["g"]},{"values":["a"]},{"values":["c"]}]}}}`)

	s2 := types.NewSet(context.Background(), suite.vs,
		types.NewStruct("", types.StructData{
			"n": types.String("a"),
		}),
		types.NewStruct("", types.StructData{
			"n": types.String("c"),
		}),
		types.NewStruct("", types.StructData{
			"n": types.String("e"),
		}),
		types.NewStruct("", types.StructData{
			"n": types.String("g"),
		}),
	)

	suite.assertQueryResult(s2, `{root{values(key: {n: "e"}) { n } }}`,
		`{"data":{"root":{"values":[{"n": "e"}]}}}`)
	suite.assertQueryResult(s2, `{root{values(key: {n: "x"}) { n } }}`, `{"data":{"root":{"values":[]}}}`)
	// The order is based on hash
	suite.assertQueryResult(s2, `{root{values(key: {n: "c"}, through: {n: "e"}) { n }}}`,
		`{"data":{"root":{"values":[{"n": "c"}, {"n": "e"}]}}}`)
}

func (suite *QueryGraphQLSuite) TestInputToNomsValue() {
	test := func(expected types.Value, val interface{}) {
		suite.True(expected.Equals(InputToNomsValue(context.Background(), suite.vs, val, types.TypeOf(expected))))
	}

	test(types.Float(42), int(42))
	test(types.Float(0), int(0))

	test(types.Float(1.23), float64(1.23))
	test(types.Float(0), float64(0))

	test(types.Bool(true), true)
	test(types.Bool(false), false)

	test(types.String("hi"), "hi")
	test(types.String(""), "")

	test(types.NewList(context.Background(), suite.vs, types.Float(42)), []interface{}{float64(42)})
	test(types.NewList(context.Background(), suite.vs, types.Float(1), types.Float(2)), []interface{}{float64(1), float64(2)})

	test(types.NewSet(context.Background(), suite.vs, types.Float(42)), []interface{}{float64(42)})
	test(types.NewSet(context.Background(), suite.vs, types.Float(1), types.Float(2)), []interface{}{float64(1), float64(2)})

	test(types.NewMap(context.Background(), suite.vs,
		types.String("a"), types.Float(1),
		types.String("b"), types.Float(2),
	), []interface{}{
		map[string]interface{}{"key": "a", "value": 1},
		map[string]interface{}{"key": "b", "value": 2},
	})
	test(types.NewMap(context.Background(), suite.vs,
		types.NewList(context.Background(), suite.vs, types.String("a")), types.Float(1),
		types.NewList(context.Background(), suite.vs, types.String("b")), types.Float(2),
	), []interface{}{
		map[string]interface{}{"key": []interface{}{"a"}, "value": 1},
		map[string]interface{}{"key": []interface{}{"b"}, "value": 2},
	})

	test(types.NewMap(context.Background(), suite.vs,
		types.NewStruct("S", types.StructData{"a": types.Float(1)}), types.Float(11),
		types.NewStruct("S", types.StructData{"a": types.Float(2)}), types.Float(22),
	), []interface{}{
		map[string]interface{}{"key": map[string]interface{}{"a": float64(1)}, "value": 11},
		map[string]interface{}{"key": map[string]interface{}{"a": float64(2)}, "value": 22},
	})

	test(types.NewSet(context.Background(), suite.vs,
		types.NewStruct("S", types.StructData{"a": types.Float(1)}),
		types.NewStruct("S", types.StructData{"a": types.Float(2)}),
	), []interface{}{
		map[string]interface{}{"a": float64(1)},
		map[string]interface{}{"a": float64(2)},
	})

	expected := types.NewStruct("S", types.StructData{
		"x": types.Float(42),
	})
	expectedType := types.MakeStructType("S",
		types.StructField{Name: "a", Type: types.BoolType, Optional: true},
		types.StructField{Name: "x", Type: types.FloaTType, Optional: false},
	)
	val := map[string]interface{}{
		"x": float64(42),
	}
	suite.Equal(expected, InputToNomsValue(context.Background(), suite.vs, val, expectedType))

	val = map[string]interface{}{
		"x": float64(42),
		"a": nil,
	}
	suite.Equal(expected, InputToNomsValue(context.Background(), suite.vs, val, expectedType))

	val = map[string]interface{}{
		"x": nil,
	}
	suite.Panics(func() {
		InputToNomsValue(context.Background(), suite.vs, val, expectedType)
	})
}

func (suite *QueryGraphQLSuite) TestErrorsInInputType() {
	ut := types.MakeUnionType(types.BoolType, types.FloaTType)

	test := func(t *types.Type) {
		tm := NewTypeMap()
		_, err := NomsTypeToGraphQLInputType(context.Background(), t, tm)
		suite.Error(err)
	}

	test(ut)
	test(types.MakeListType(ut))
	test(types.MakeSetType(ut))
	test(types.MakeMapType(ut, types.BoolType))
	test(types.MakeMapType(types.BoolType, ut))
	test(types.MakeMapType(ut, ut))
	test(types.MakeStructTypeFromFields("", types.FieldMap{"u": ut}))

	test(types.MakeStructTypeFromFields("S", types.FieldMap{
		"l": types.MakeListType(types.MakeCycleType("S")),
	}))
	test(types.MakeStructTypeFromFields("S", types.FieldMap{
		"n": types.FloaTType,
		"l": types.MakeListType(types.MakeCycleType("S")),
	}))
}

func (suite *QueryGraphQLSuite) TestVariables() {
	test := func(rootValue types.Value, expected string, query string, vars map[string]interface{}) {
		tc := NewTypeConverter()
		ctx := NewContext(context.Background(), suite.vs)
		schema, err := graphql.NewSchema(graphql.SchemaConfig{
			Query: tc.NewRootQueryObject(context.Background(), rootValue),
		})
		suite.NoError(err)

		r := graphql.Do(graphql.Params{
			Schema:         schema,
			RequestString:  query,
			Context:        ctx,
			VariableValues: vars,
		})
		b, err := json.Marshal(r)
		suite.NoError(err)
		suite.JSONEq(expected, string(b))
	}

	v := types.NewList(context.Background(), suite.vs, types.Float(0), types.Float(1), types.Float(2), types.Float(3))
	test(v, `{"data":{"root":{"values":[0,1,2,3]}}}`, `query Test($c: Int) { root { values(count: $c) } }`, nil)
	test(v, `{"data":{"root":{"values":[0,1]}}}`, `query Test($c: Int) { root { values(count: $c) } }`, map[string]interface{}{
		"c": 2,
	})

	m := types.NewMap(context.Background(), suite.vs,
		types.String("a"), types.Float(0),
		types.String("b"), types.Float(1),
		types.String("c"), types.Float(2),
		types.String("d"), types.Float(3),
	)
	test(m, `{"data":{"root":{"values":[1]}}}`, `query Test($k: String) { root { values(key: $k) } }`, map[string]interface{}{
		"k": "b",
	})
	test(m, `{"data":{"root":{"values":[1, 2]}}}`, `query Test($k: String, $t: String) { root { values(key: $k, through: $t) } }`,
		map[string]interface{}{
			"k": "b",
			"t": "c",
		})
	test(m, `{"data":{"root":{"values":[0, 2]}}}`, `query Test($ks: [String!]!) { root { values(keys: $ks) } }`,
		map[string]interface{}{
			"ks": []string{"a", "c"},
		})

	m2 := types.NewMap(context.Background(), suite.vs,
		types.NewStruct("S", types.StructData{"n": types.String("a")}), types.Float(0),
		types.NewStruct("S", types.StructData{"n": types.String("b")}), types.Float(1),
		types.NewStruct("S", types.StructData{"n": types.String("c")}), types.Float(2),
		types.NewStruct("S", types.StructData{"n": types.String("d")}), types.Float(3),
	)
	keyType := types.TypeOf(m2).Desc.(types.CompoundDesc).ElemTypes[0]
	q := fmt.Sprintf(`query Test($k: %s) { root { values(key: $k) } }`, GetInputTypeName(context.Background(), keyType))
	test(m2, `{"data":{"root":{"values":[1]}}}`, q, map[string]interface{}{
		"k": map[string]interface{}{
			"n": "b",
		},
	})
	q = fmt.Sprintf(`query Test($ks: [%s!]) { root { values(keys: $ks) } }`, GetInputTypeName(context.Background(), keyType))
	test(m2, `{"data":{"root":{"values":[0, 3]}}}`, q, map[string]interface{}{
		"ks": []interface{}{
			map[string]interface{}{
				"n": "a",
			},
			map[string]interface{}{
				"n": "d",
			},
		},
	})
	test(m2, `{"data":null,"errors":[{"message":"Variable \"$ks\" got invalid value [{}].\nIn element #1: In field \"n\": Expected \"String!\", found null.","locations":[{"line":1,"column":12}]}]}`,
		q,
		map[string]interface{}{
			"ks": []interface{}{
				map[string]interface{}{},
			},
		},
	)
	test(m2, `{"data":null,"errors":[{"message":"Variable \"$ks\" got invalid value [{\"m\":\"b\",\"n\":\"a\"}].\nIn element #1: In field \"m\": Unknown field.","locations":[{"line":1,"column":12}]}]}`,
		q,
		map[string]interface{}{
			"ks": []interface{}{
				map[string]interface{}{
					"n": "a",
					"m": "b",
				},
			},
		},
	)
	test(m2, `{"data":null,"errors":[{"message":"Variable \"$ks\" got invalid value [{\"n\":null}].\nIn element #1: In field \"n\": Expected \"String!\", found null.","locations":[{"line":1,"column":12}]}]}`,
		q,
		map[string]interface{}{
			"ks": []interface{}{
				map[string]interface{}{
					"n": nil,
				},
			},
		},
	)
	test(m2, `{"data":null,"errors":[{"message":"Variable \"$ks\" got invalid value [null].\nIn element #1: Expected \"SInput_cgmdbo!\", found null.","locations":[{"line":1,"column":12}]}]}`,
		q,
		map[string]interface{}{
			"ks": []interface{}{
				nil,
			},
		},
	)

	m3 := types.NewMap(context.Background(), suite.vs,
		types.NewMap(context.Background(), suite.vs, types.Float(0), types.String("zero")), types.Bool(false),
		types.NewMap(context.Background(), suite.vs, types.Float(1), types.String("one")), types.Bool(true),
	)
	keyNomsType := types.TypeOf(m3).Desc.(types.CompoundDesc).ElemTypes[0]
	tc := NewTypeConverter()
	keyGraphQLInputType, err := tc.NomsTypeToGraphQLInputType(context.Background(), keyNomsType)
	suite.NoError(err)
	q = fmt.Sprintf(`query Test($k: %s!) { root { values(key: $k) } }`, keyGraphQLInputType.String())
	test(m3, `{"data":{"root":{"values":[false]}}}`, q, map[string]interface{}{
		"k": []interface{}{
			map[string]interface{}{
				"key":   float64(0),
				"value": "zero",
			},
		},
	})
	test(m3, `{"data":null,"errors":[{"message":"Variable \"$k\" got invalid value [{\"key\":0}].\nIn element #1: In field \"value\": Expected \"String!\", found null.","locations":[{"line":1,"column":12}]}]}`,
		q,
		map[string]interface{}{
			"k": []interface{}{
				map[string]interface{}{
					"key": float64(0),
				},
			},
		})
	test(m3, `{"data":null,"errors":[{"message":"Variable \"$k\" got invalid value [{\"key\":\"zero\"}].\nIn element #1: In field \"key\": Expected type \"Float\", found \"zero\".\nIn element #2: In field \"value\": Expected \"String!\", found null.","locations":[{"line":1,"column":12}]}]}`,
		q,
		map[string]interface{}{
			"k": []interface{}{
				map[string]interface{}{
					"key": "zero",
				},
			},
		})
	test(m3, `{"data":null,"errors":[{"message":"Variable \"$k\" got invalid value [{\"extra\":false,\"key\":0,\"value\":\"zero\"}].\nIn element #1: In field \"extra\": Unknown field.","locations":[{"line":1,"column":12}]}]}`,
		q,
		map[string]interface{}{
			"k": []interface{}{
				map[string]interface{}{
					"key":   float64(0),
					"value": "zero",
					"extra": false,
				},
			},
		})
	test(m3, `{"data":null,"errors":[{"message":"Variable \"$k\" got invalid value [null].\nIn element #1: Expected \"FloatStringEntryInput!\", found null.","locations":[{"line":1,"column":12}]}]}`,
		q,
		map[string]interface{}{
			"k": []interface{}{
				nil,
			},
		})
}

func (suite *QueryGraphQLSuite) TestNameFunc() {
	test := func(tc *TypeConverter, rootValue types.Value, expected string, query string, vars map[string]interface{}) {
		ctx := NewContext(context.Background(), suite.vs)
		schema, err := graphql.NewSchema(graphql.SchemaConfig{
			Query: tc.NewRootQueryObject(context.Background(), rootValue),
		})
		suite.NoError(err)

		r := graphql.Do(graphql.Params{
			Schema:         schema,
			RequestString:  query,
			Context:        ctx,
			VariableValues: vars,
		})

		b, err := json.Marshal(r)
		suite.NoError(err)
		suite.JSONEq(expected, string(b))
	}

	aVal := types.NewStruct("A", types.StructData{
		"a": types.Float(1),
	})
	bVal := types.NewStruct("B", types.StructData{
		"b": types.Float(2),
	})

	list := types.NewList(context.Background(), suite.vs, aVal, bVal)

	tc := NewTypeConverter()
	tc.NameFunc = func(ctx context.Context, nomsType *types.Type, isInputType bool) string {
		if nomsType.Equals(types.TypeOf(aVal)) {
			return "A"
		}
		if nomsType.Equals(types.TypeOf(bVal)) {
			return "BBB"
		}
		return DefaultNameFunc(ctx, nomsType, isInputType)
	}

	query := `query {
                root {
                        values {
                                ... on A {
                                        a
                                }
                                ... on BBB {
                                        b
                                }
                        }
                }
        }`
	expected := `{
                "data": {
                        "root": {
                                "values": [
                                        {"a": 1},
                                        {"b": 2}
                                ]
                        }
                }
        }`
	test(tc, list, expected, query, nil)

	set := types.NewSet(context.Background(), suite.vs, aVal,
		types.NewStruct("A", types.StructData{
			"a": types.Float(2),
		}),
		types.NewStruct("A", types.StructData{
			"a": types.Float(3),
		}),
	)
	tc = NewTypeConverter()
	tc.NameFunc = func(ctx context.Context, nomsType *types.Type, isInputType bool) string {
		if nomsType.Equals(types.TypeOf(aVal)) {
			if isInputType {
				return "AI"
			}
			return "A"
		}
		return DefaultNameFunc(ctx, nomsType, isInputType)
	}

	query = `query ($key: AI!) {
                root {
                        values(key: $key) {
                                a
                        }
                }
        }`
	expected = `{
                "data": {
                        "root": {
                                "values": [
                                        {"a": 2}
                                ]
                        }
                }
        }`
	test(tc, set, expected, query, map[string]interface{}{
		"key": map[string]interface{}{"a": 2},
	})
}

func TestGetListElementsWithSet(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()
	v := types.NewSet(context.Background(), vs, types.Float(0), types.Float(1), types.Float(2))
	r := getListElements(context.Background(), vs, v, map[string]interface{}{})
	assert.Equal([]interface{}{float64(0), float64(1), float64(2)}, r)

	r = getListElements(context.Background(), vs, v, map[string]interface{}{
		atKey: 1,
	})
	assert.Equal([]interface{}{float64(1), float64(2)}, r)

	r = getListElements(context.Background(), vs, v, map[string]interface{}{
		countKey: 2,
	})
	assert.Equal([]interface{}{float64(0), float64(1)}, r)
}

func TestNoErrorOnNonCyclicTypeRefsInputType(t *testing.T) {
	assert := assert.New(t)

	type User struct {
		ID string `noms:"id"`
	}
	type Account struct {
		PendingUsers map[string]User
		Users        map[string]User
	}

	var a Account
	typ := marshal.MustMarshalType(a)
	tc := NewTypeConverter()
	_, err := tc.NomsTypeToGraphQLInputType(context.Background(), typ)
	assert.NoError(err)
}

func TestErrorOnCyclicTypeRefsInputType(t *testing.T) {
	assert := assert.New(t)

	type Node struct {
		Children map[string]Node
	}

	var n Node
	typ := marshal.MustMarshalType(n)
	tc := NewTypeConverter()
	_, err := tc.NomsTypeToGraphQLInputType(context.Background(), typ)
	assert.Error(err)
}
