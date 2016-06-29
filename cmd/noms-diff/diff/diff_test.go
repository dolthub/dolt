// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
	"github.com/syndtr/goleveldb/leveldb/util"
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
	fields := map[string]types.Value{}
	for i := 0; i < len(kv); i += 2 {
		fields[kv[i].(string)] = valToTypesValue(kv[i+1])
	}
	return types.NewStruct(name, fields)
}

func TestNomsMapdiff(t *testing.T) {
	assert := assert.New(t)
	expected := "./.\"map-3\" {\n-   \"m3\": \"m-three\"\n+   \"m3\": \"m-three-diff\"\n  }\n./.\"map-3\".\"m4\" {\n-   \"a1\": \"a-one\"\n+   \"a1\": \"a-one-diff\"\n  }\n"

	m1 := createMap("map-1", mm1, "map-2", mm2, "map-3", mm3, "map-4", mm4)
	m2 := createMap("map-1", mm1, "map-2", mm2, "map-3", mm3x, "map-4", mm4)
	buf := util.NewBuffer(nil)
	Diff(buf, m1, m2)
	assert.Equal(expected, buf.String())
}

func TestNomsSetDiff(t *testing.T) {
	assert := assert.New(t)

	expected := "./ {\n-   \"five\"\n+   \"five-diff\"\n  }\n"
	s1 := createSet("one", "three", "five", "seven", "nine")
	s2 := createSet("one", "three", "five-diff", "seven", "nine")
	buf := util.NewBuffer(nil)
	Diff(buf, s1, s2)
	assert.Equal(expected, buf.String())

	expected = `./ {
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
  }
`
	s1 = createSet(mm1, mm2, mm3, mm4)
	s2 = createSet(mm1, mm2, mm3x, mm4)
	buf = util.NewBuffer(nil)
	Diff(buf, s1, s2)
	assert.Equal(expected, buf.String())
}

func TestNomsStructDiff(t *testing.T) {
	assert := assert.New(t)
	expected := "./ {\n-   \"four\": \"four\"\n+   \"four\": \"four-diff\"\n  }\n./.\"three\" {\n-   \"field3\": \"field3-data\"\n+   \"field3\": \"field3-data-diff\"\n"

	fieldData := []interface{}{
		"field1", "field1-data",
		"field2", "field2-data",
		"field3", "field3-data",
		"field4", "field4-data",
	}
	s1 := createStruct("TestData", fieldData...)
	s2 := s1.Set("field3", types.String("field3-data-diff"))

	m1 := createMap("one", 1, "two", 2, "three", s1, "four", "four")
	m2 := createMap("one", 1, "two", 2, "three", s2, "four", "four-diff")

	buf := util.NewBuffer(nil)
	Diff(buf, m1, m2)
	assert.Equal(expected, buf.String())
}

func TestNomsListDiff(t *testing.T) {
	assert := assert.New(t)

	expected := "./ {\n-   2\n+   22\n-   44\n  }\n"
	l1 := createList(1, 2, 3, 4, 44, 5, 6)
	l2 := createList(1, 22, 3, 4, 5, 6)
	buf := util.NewBuffer(nil)
	Diff(buf, l1, l2)
	assert.Equal(expected, buf.String())

	expected = "./ {\n+   \"seven\"\n  }\n"
	l1 = createList("one", "two", "three", "four", "five", "six")
	l2 = createList("one", "two", "three", "four", "five", "six", "seven")
	buf = util.NewBuffer(nil)
	Diff(buf, l1, l2)
	assert.Equal(expected, buf.String())

	expected = "./[2] {\n-   \"m3\": \"m-three\"\n+   \"m3\": \"m-three-diff\"\n  }\n./[2].\"m4\" {\n-   \"a1\": \"a-one\"\n+   \"a1\": \"a-one-diff\"\n  }\n"
	l1 = createList(mm1, mm2, mm3, mm4)
	l2 = createList(mm1, mm2, mm3x, mm4)
	buf = util.NewBuffer(nil)
	Diff(buf, l1, l2)
	assert.Equal(expected, buf.String())
}
