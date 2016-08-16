// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"bytes"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/test"
	"github.com/attic-labs/testify/assert"
)

var (
	aa1  = createMap("a1", "a-one", "a2", "a-two", "a3", "a-three", "a4", "a-four")
	aa1x = createMap("a1", "a-one-diff", "a2", "a-two", "a3", "a-three", "a4", "a-four")

	mm1  = createMap("k1", "k-one", "k2", "k-two", "k3", "k-three", "k4", aa1)
	mm2  = createMap("l1", "l-one", "l2", "l-two", "l3", "l-three", "l4", aa1)
	mm3  = createMap("m1", "m-one", "v2", "m-two", "m3", "m-three", "m4", aa1)
	mm3x = createMap("m1", "m-one", "v2", "m-two", "m3", "m-three-diff", "m4", aa1x)
	mm4  = createMap("n1", "n-one", "n2", "n-two", "n3", "n-three", "n4", aa1)
)

func valToTypesValue(v interface{}) types.Value {
	var v1 types.Value
	switch t := v.(type) {
	case string:
		v1 = types.String(t)
	case int:
		v1 = types.Number(t)
	case types.Value:
		v1 = t
	}
	return v1
}

func valsToTypesValues(kv ...interface{}) []types.Value {
	keyValues := []types.Value{}
	for _, e := range kv {
		v := valToTypesValue(e)
		keyValues = append(keyValues, v)
	}
	return keyValues
}

func createMap(kv ...interface{}) types.Map {
	keyValues := valsToTypesValues(kv...)
	return types.NewMap(keyValues...)
}

func createSet(kv ...interface{}) types.Set {
	keyValues := valsToTypesValues(kv...)
	return types.NewSet(keyValues...)
}

func createList(kv ...interface{}) types.List {
	keyValues := valsToTypesValues(kv...)
	return types.NewList(keyValues...)
}

func createStruct(name string, kv ...interface{}) types.Struct {
	fields := types.StructData{}
	for i := 0; i < len(kv); i += 2 {
		fields[kv[i].(string)] = valToTypesValue(kv[i+1])
	}
	return types.NewStruct(name, fields)
}

func TestNomsMapdiff(t *testing.T) {
	assert := assert.New(t)
	expected := `["map-3"] {
-   "m3": "m-three"
+   "m3": "m-three-diff"
  }
["map-3"]["m4"] {
-   "a1": "a-one"
+   "a1": "a-one-diff"
  }
`

	tf := func(leftRight bool) {
		m1 := createMap("map-1", mm1, "map-2", mm2, "map-3", mm3, "map-4", mm4)
		m2 := createMap("map-1", mm1, "map-2", mm2, "map-3", mm3x, "map-4", mm4)
		buf := &bytes.Buffer{}
		Diff(buf, m1, m2, leftRight)

		assert.Equal(expected, buf.String())
	}

	tf(true)
	tf(false)
}

func TestNomsSetDiff(t *testing.T) {
	assert := assert.New(t)

	expected1 := `(root) {
-   "five"
+   "five-diff"
  }
`

	expected2 := `(root) {
+   {  // 4 items
+     "m1": "m-one",
+     "m3": "m-three-diff",
+     "m4": {  // 4 items
+       "a1": "a-one-diff",
+       "a2": "a-two",
+       "a3": "a-three",
+       "a4": "a-four",
+     },
+     "v2": "m-two",
+   }
-   {  // 4 items
-     "m1": "m-one",
-     "m3": "m-three",
-     "m4": {  // 4 items
-       "a1": "a-one",
-       "a2": "a-two",
-       "a3": "a-three",
-       "a4": "a-four",
-     },
-     "v2": "m-two",
-   }
  }
`

	s1 := createSet("one", "three", "five", "seven", "nine")
	s2 := createSet("one", "three", "five-diff", "seven", "nine")
	s3 := createSet(mm1, mm2, mm3, mm4)
	s4 := createSet(mm1, mm2, mm3x, mm4)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		Diff(buf, s1, s2, leftRight)
		assert.Equal(expected1, buf.String())

		buf = &bytes.Buffer{}
		Diff(buf, s3, s4, leftRight)
		assert.Equal(expected2, buf.String())
	}

	tf(true)
	tf(false)
}

func TestNomsStructDiff(t *testing.T) {
	assert := assert.New(t)

	expected1 := `(root) {
-   "four": "four"
+   "four": "four-diff"
  }
["three"] {
-   field1: "field1-data"
-   field3: "field3-data"
+   field3: "field3-data-diff"
+   field4: "field4-data"
  }
`

	expected2 := `(root) {
-   four: "four"
+   four: "four-diff"
  }
.three {
-   field1: "field1-data"
-   field3: "field3-data"
+   field3: "field3-data-diff"
+   field4: "field4-data"
  }
`

	s1 := createStruct("TestData",
		"field1", "field1-data",
		"field2", "field2-data",
		"field3", "field3-data",
	)
	s2 := createStruct("TestData",
		"field2", "field2-data",
		"field3", "field3-data-diff",
		"field4", "field4-data",
	)

	m1 := createMap("one", 1, "two", 2, "three", s1, "four", "four")
	m2 := createMap("one", 1, "two", 2, "three", s2, "four", "four-diff")

	s3 := createStruct("", "one", 1, "two", 2, "three", s1, "four", "four")
	s4 := createStruct("", "one", 1, "two", 2, "three", s2, "four", "four-diff")

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		Diff(buf, m1, m2, leftRight)
		assert.Equal(expected1, buf.String())

		buf = &bytes.Buffer{}
		Diff(buf, s3, s4, leftRight)
		assert.Equal(expected2, buf.String())
	}

	tf(true)
	tf(false)
}

func TestNomsListDiff(t *testing.T) {
	assert := assert.New(t)

	expected1 := `(root) {
-   2
+   22
-   44
  }
`

	l1 := createList(1, 2, 3, 4, 44, 5, 6)
	l2 := createList(1, 22, 3, 4, 5, 6)

	expected2 := `(root) {
+   "seven"
  }
`

	l3 := createList("one", "two", "three", "four", "five", "six")
	l4 := createList("one", "two", "three", "four", "five", "six", "seven")

	expected3 := `[2] {
-   "m3": "m-three"
+   "m3": "m-three-diff"
  }
[2]["m4"] {
-   "a1": "a-one"
+   "a1": "a-one-diff"
  }
`

	l5 := createList(mm1, mm2, mm3, mm4)
	l6 := createList(mm1, mm2, mm3x, mm4)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		Diff(buf, l1, l2, leftRight)
		assert.Equal(expected1, buf.String())

		buf = &bytes.Buffer{}
		Diff(buf, l3, l4, leftRight)
		assert.Equal(expected2, buf.String())

		buf = &bytes.Buffer{}
		Diff(buf, l5, l6, leftRight)

		assert.Equal(expected3, buf.String())
	}

	tf(true)
	tf(false)
}

func TestNomsBlobDiff(t *testing.T) {
	assert := assert.New(t)

	expected := "-   Blob (2.0 kB)\n+   Blob (11 B)\n"
	b1 := types.NewBlob(strings.NewReader(strings.Repeat("x", 2*1024)))
	b2 := types.NewBlob(strings.NewReader("Hello World"))

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		Diff(buf, b1, b2, leftRight)
		assert.Equal(expected, buf.String())
	}

	tf(true)
	tf(false)
}

func TestNomsTypeDiff(t *testing.T) {
	assert := assert.New(t)

	expected1 := "-   List<Number>\n+   List<String>\n"
	t1 := types.MakeListType(types.NumberType)
	t2 := types.MakeListType(types.StringType)

	expected2 := "-   List<Number>\n+   Set<String>\n"
	t3 := types.MakeListType(types.NumberType)
	t4 := types.MakeSetType(types.StringType)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		Diff(buf, t1, t2, leftRight)
		assert.Equal(expected1, buf.String())

		buf = &bytes.Buffer{}
		Diff(buf, t3, t4, leftRight)
		assert.Equal(expected2, buf.String())
	}

	tf(true)
	tf(false)
}

func TestNomsRefDiff(t *testing.T) {
	expected := "-   fckcbt7nk5jl4arco2dk7r9nj7abb6ci\n+   i7d3u5gekm48ot419t2cot6cnl7ltcah\n"
	l1 := createList(1)
	l2 := createList(2)
	r1 := types.NewRef(l1)
	r2 := types.NewRef(l2)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		Diff(buf, r1, r2, leftRight)

		test.EqualsIgnoreHashes(t, expected, buf.String())
	}

	tf(true)
	tf(false)
}
