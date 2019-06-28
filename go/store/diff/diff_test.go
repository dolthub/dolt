// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/test"
	"github.com/liquidata-inc/ld/dolt/go/store/util/writers"
	"github.com/stretchr/testify/assert"
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
		v1 = types.Float(t)
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
	vs := newTestValueStore()
	defer vs.Close()
	keyValues := valsToTypesValues(kv...)
	return types.NewMap(context.Background(), types.Format_7_18, vs, keyValues...)
}

func createSet(kv ...interface{}) types.Set {
	vs := newTestValueStore()
	defer vs.Close()
	keyValues := valsToTypesValues(kv...)
	return types.NewSet(context.Background(), types.Format_7_18, vs, keyValues...)
}

func createList(kv ...interface{}) types.List {
	vs := newTestValueStore()
	defer vs.Close()
	keyValues := valsToTypesValues(kv...)
	return types.NewList(context.Background(), types.Format_7_18, vs, keyValues...)
}

func createStruct(name string, kv ...interface{}) types.Struct {
	fields := types.StructData{}
	for i := 0; i < len(kv); i += 2 {
		fields[kv[i].(string)] = valToTypesValue(kv[i+1])
	}
	return types.NewStruct(types.Format_7_18, name, fields)
}

func pathsFromDiff(v1, v2 types.Value, leftRight bool) []string {
	dChan := make(chan Difference)
	sChan := make(chan struct{})

	go func() {
		Diff(context.Background(), v1, v2, dChan, sChan, leftRight, nil)
		close(dChan)
	}()

	var paths []string
	for d := range dChan {
		paths = append(paths, d.Path.String())
	}
	return paths
}

func mustParsePath(assert *assert.Assertions, s string) types.Path {
	if s == "" {
		return nil
	}
	p, err := types.ParsePath(s, types.Format_7_18)
	assert.NoError(err)
	return p
}

func TestNomsDiffPrintMap(t *testing.T) {
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
	expectedPaths := []string{
		`["map-3"]["m3"]`,
		`["map-3"]["m4"]["a1"]`,
	}

	tf := func(leftRight bool) {
		m1 := createMap("map-1", mm1, "map-2", mm2, "map-3", mm3, "map-4", mm4)
		m2 := createMap("map-1", mm1, "map-2", mm2, "map-3", mm3x, "map-4", mm4)
		buf := &bytes.Buffer{}
		PrintDiff(context.Background(), buf, m1, m2, leftRight)
		assert.Equal(expected, buf.String())

		paths := pathsFromDiff(m1, m2, leftRight)
		assert.Equal(expectedPaths, paths)
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintSet(t *testing.T) {
	assert := assert.New(t)

	expected1 := `(root) {
-   "five"
+   "five-diff"
  }
`
	expectedPaths1 := []string{
		`["five"]`,
		`["five-diff"]`,
	}

	expected2 := `(root) {
-   map {  // 4 items
-     "m1": "m-one",
-     "m3": "m-three",
-     "m4": map {  // 4 items
-       "a1": "a-one",
-       "a2": "a-two",
-       "a3": "a-three",
-       "a4": "a-four",
-     },
-     "v2": "m-two",
-   }
+   map {  // 4 items
+     "m1": "m-one",
+     "m3": "m-three-diff",
+     "m4": map {  // 4 items
+       "a1": "a-one-diff",
+       "a2": "a-two",
+       "a3": "a-three",
+       "a4": "a-four",
+     },
+     "v2": "m-two",
+   }
  }
`
	expectedPaths2 := []string{
		// TODO(binformat)
		fmt.Sprintf("[#%s]", mm3.Hash(types.Format_7_18)),
		fmt.Sprintf("[#%s]", mm3x.Hash(types.Format_7_18)),
	}

	s1 := createSet("one", "three", "five", "seven", "nine")
	s2 := createSet("one", "three", "five-diff", "seven", "nine")
	s3 := createSet(mm1, mm2, mm3, mm4)
	s4 := createSet(mm1, mm2, mm3x, mm4)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		PrintDiff(context.Background(), buf, s1, s2, leftRight)
		assert.Equal(expected1, buf.String())

		paths := pathsFromDiff(s1, s2, leftRight)
		assert.Equal(expectedPaths1, paths)

		buf = &bytes.Buffer{}
		PrintDiff(context.Background(), buf, s3, s4, leftRight)
		assert.Equal(expected2, buf.String())

		paths = pathsFromDiff(s3, s4, leftRight)
		assert.Equal(expectedPaths2, paths)
	}

	tf(true)
	tf(false)
}

// This function tests stop functionality in PrintDiff and Diff.
func TestNomsDiffPrintStop(t *testing.T) {
	assert := assert.New(t)

	expected1 := `(root) {
-   "five"
`

	expected2 := `(root) {
-   map {  // 4 items
`

	s1 := createSet("one", "three", "five", "seven", "nine")
	s2 := createSet("one", "three", "five-diff", "seven", "nine")
	s3 := createSet(mm1, mm2, mm3, mm4)
	s4 := createSet(mm1, mm2, mm3x, mm4)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		mlw := &writers.MaxLineWriter{Dest: buf, MaxLines: 2}
		PrintDiff(context.Background(), mlw, s1, s2, leftRight)
		assert.Equal(expected1, buf.String())

		buf = &bytes.Buffer{}
		mlw = &writers.MaxLineWriter{Dest: buf, MaxLines: 2}
		PrintDiff(context.Background(), mlw, s3, s4, leftRight)
		assert.Equal(expected2, buf.String())
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintStruct(t *testing.T) {
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
	expectedPaths1 := []string{
		`["four"]`,
		`["three"].field1`,
		`["three"].field3`,
		`["three"].field4`,
	}

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
	expectedPaths2 := []string{
		`.four`,
		`.three.field1`,
		`.three.field3`,
		`.three.field4`,
	}

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
		PrintDiff(context.Background(), buf, m1, m2, leftRight)
		assert.Equal(expected1, buf.String())

		paths := pathsFromDiff(m1, m2, leftRight)
		assert.Equal(expectedPaths1, paths)

		buf = &bytes.Buffer{}
		PrintDiff(context.Background(), buf, s3, s4, leftRight)
		assert.Equal(expected2, buf.String())

		paths = pathsFromDiff(s3, s4, leftRight)
		assert.Equal(expectedPaths2, paths)
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintMapWithStructKeys(t *testing.T) {
	a := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	k1 := createStruct("TestKey", "name", "n1", "label", "l1")

	expected1 := `(root) {
-   struct TestKey {
-     label: "l1",
-     name: "n1",
-   }: true
+   struct TestKey {
+     label: "l1",
+     name: "n1",
+   }: false
  }
`

	m1 := types.NewMap(context.Background(), types.Format_7_18, vs, k1, types.Bool(true))
	m2 := types.NewMap(context.Background(), types.Format_7_18, vs, k1, types.Bool(false))
	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		PrintDiff(context.Background(), buf, m1, m2, leftRight)
		a.Equal(expected1, buf.String())
	}
	tf(true)
	tf(false)
}

func TestNomsDiffPrintList(t *testing.T) {
	assert := assert.New(t)

	expected1 := `(root) {
-   2
+   22
-   44
  }
`
	expectedPaths1 := []string{
		`[1]`,
		`[4]`,
	}

	l1 := createList(1, 2, 3, 4, 44, 5, 6)
	l2 := createList(1, 22, 3, 4, 5, 6)

	expected2 := `(root) {
+   "seven"
  }
`
	expectedPaths2 := []string{
		`[6]`,
	}

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
	expectedPaths3 := []string{
		`[2]["m3"]`,
		`[2]["m4"]["a1"]`,
	}

	l5 := createList(mm1, mm2, mm3, mm4)
	l6 := createList(mm1, mm2, mm3x, mm4)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		PrintDiff(context.Background(), buf, l1, l2, leftRight)
		assert.Equal(expected1, buf.String())

		paths := pathsFromDiff(l1, l2, leftRight)
		assert.Equal(expectedPaths1, paths)

		buf = &bytes.Buffer{}
		PrintDiff(context.Background(), buf, l3, l4, leftRight)
		assert.Equal(expected2, buf.String())

		paths = pathsFromDiff(l3, l4, leftRight)
		assert.Equal(expectedPaths2, paths)

		buf = &bytes.Buffer{}
		PrintDiff(context.Background(), buf, l5, l6, leftRight)
		assert.Equal(expected3, buf.String())

		paths = pathsFromDiff(l5, l6, leftRight)
		assert.Equal(expectedPaths3, paths)
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintBlob(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	expected := "-   Blob (2.0 kB)\n+   Blob (11 B)\n"
	expectedPaths1 := []string{``}
	// TODO(binformat)
	b1 := types.NewBlob(context.Background(), types.Format_7_18, vs, strings.NewReader(strings.Repeat("x", 2*1024)))
	b2 := types.NewBlob(context.Background(), types.Format_7_18, vs, strings.NewReader("Hello World"))

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		PrintDiff(context.Background(), buf, b1, b2, leftRight)
		assert.Equal(expected, buf.String())

		paths := pathsFromDiff(b1, b2, leftRight)
		assert.Equal(expectedPaths1, paths)
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintType(t *testing.T) {
	assert := assert.New(t)

	expected1 := "-   List<Float>\n+   List<String>\n"
	expectedPaths1 := []string{""}
	t1 := types.MakeListType(types.FloaTType)
	t2 := types.MakeListType(types.StringType)

	expected2 := "-   List<Float>\n+   Set<String>\n"
	expectedPaths2 := []string{``}
	t3 := types.MakeListType(types.FloaTType)
	t4 := types.MakeSetType(types.StringType)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		PrintDiff(context.Background(), buf, t1, t2, leftRight)
		assert.Equal(expected1, buf.String())

		paths := pathsFromDiff(t1, t2, leftRight)
		assert.Equal(expectedPaths1, paths)

		buf = &bytes.Buffer{}
		PrintDiff(context.Background(), buf, t3, t4, leftRight)
		assert.Equal(expected2, buf.String())

		paths = pathsFromDiff(t3, t4, leftRight)
		assert.Equal(expectedPaths2, paths)
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintRef(t *testing.T) {
	assert := assert.New(t)

	expected := "-   #fckcbt7nk5jl4arco2dk7r9nj7abb6ci\n+   #i7d3u5gekm48ot419t2cot6cnl7ltcah\n"
	expectedPaths1 := []string{``}
	l1 := createList(1)
	l2 := createList(2)
	r1 := types.NewRef(l1, types.Format_7_18)
	r2 := types.NewRef(l2, types.Format_7_18)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		PrintDiff(context.Background(), buf, r1, r2, leftRight)
		test.EqualsIgnoreHashes(t, expected, buf.String())

		paths := pathsFromDiff(r1, r2, leftRight)
		assert.Equal(expectedPaths1, paths)
	}

	tf(true)
	tf(false)
}
