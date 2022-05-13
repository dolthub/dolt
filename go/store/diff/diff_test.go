// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/test"
	"github.com/dolthub/dolt/go/store/util/writers"
)

var (
	aa1  types.Map
	aa1x types.Map

	mm1  types.Map
	mm2  types.Map
	mm3  types.Map
	mm3x types.Map
	mm4  types.Map
)

func initmaps(vs types.ValueReadWriter) {
	aa1 = createMap(vs, "a1", "a-one", "a2", "a-two", "a3", "a-three", "a4", "a-four")
	aa1x = createMap(vs, "a1", "a-one-diff", "a2", "a-two", "a3", "a-three", "a4", "a-four")

	mm1 = createMap(vs, "k1", "k-one", "k2", "k-two", "k3", "k-three", "k4", aa1)
	mm2 = createMap(vs, "l1", "l-one", "l2", "l-two", "l3", "l-three", "l4", aa1)
	mm3 = createMap(vs, "m1", "m-one", "v2", "m-two", "m3", "m-three", "m4", aa1)
	mm3x = createMap(vs, "m1", "m-one", "v2", "m-two", "m3", "m-three-diff", "m4", aa1x)
	mm4 = createMap(vs, "n1", "n-one", "n2", "n-two", "n3", "n-three", "n4", aa1)
}

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

func createMap(vs types.ValueReadWriter, kv ...interface{}) types.Map {
	keyValues := valsToTypesValues(kv...)
	m, err := types.NewMap(context.Background(), vs, keyValues...)
	d.PanicIfError(err)
	return m
}

func createSet(vs types.ValueReadWriter, kv ...interface{}) types.Set {
	keyValues := valsToTypesValues(kv...)
	s, err := types.NewSet(context.Background(), vs, keyValues...)
	d.PanicIfError(err)
	return s
}

func createList(vs types.ValueReadWriter, kv ...interface{}) types.List {
	keyValues := valsToTypesValues(kv...)
	l, err := types.NewList(context.Background(), vs, keyValues...)
	d.PanicIfError(err)
	return l
}

func createStruct(vs types.ValueReadWriter, name string, kv ...interface{}) types.Struct {
	fields := types.StructData{}
	for i := 0; i < len(kv); i += 2 {
		fields[kv[i].(string)] = valToTypesValue(kv[i+1])
	}
	st, err := types.NewStruct(vs.Format(), name, fields)
	d.PanicIfError(err)
	return st
}

func pathsFromDiff(v1, v2 types.Value, leftRight bool) ([]string, error) {
	var derr error
	dChan := make(chan Difference)
	go func() {
		defer close(dChan)
		derr = Diff(context.Background(), v1, v2, dChan, leftRight, nil)
	}()

	var paths []string
	for d := range dChan {
		paths = append(paths, d.Path.String())
	}

	return paths, derr
}

func mustParsePath(assert *assert.Assertions, s string) types.Path {
	if s == "" {
		return nil
	}
	p, err := types.ParsePath(s)
	assert.NoError(err)
	return p
}

func TestNomsDiffPrintMap(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()
	initmaps(vs)

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
		m1 := createMap(vs, "map-1", mm1, "map-2", mm2, "map-3", mm3, "map-4", mm4)
		m2 := createMap(vs, "map-1", mm1, "map-2", mm2, "map-3", mm3x, "map-4", mm4)
		buf := &bytes.Buffer{}
		PrintDiff(context.Background(), buf, m1, m2, leftRight)
		assert.Equal(expected, buf.String())

		paths, err := pathsFromDiff(m1, m2, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths, paths)
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintSet(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()

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
	h3, err := mm3.Hash(vs.Format())
	require.NoError(t, err)
	h3x, err := mm3x.Hash(vs.Format())
	require.NoError(t, err)
	expectedPaths2 := []string{
		fmt.Sprintf("[#%s]", h3),
		fmt.Sprintf("[#%s]", h3x),
	}

	s1 := createSet(vs, "one", "three", "five", "seven", "nine")
	s2 := createSet(vs, "one", "three", "five-diff", "seven", "nine")
	s3 := createSet(vs, mm1, mm2, mm3, mm4)
	s4 := createSet(vs, mm1, mm2, mm3x, mm4)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		PrintDiff(context.Background(), buf, s1, s2, leftRight)
		assert.Equal(expected1, buf.String())

		paths, err := pathsFromDiff(s1, s2, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths1, paths)

		buf = &bytes.Buffer{}
		err = PrintDiff(context.Background(), buf, s3, s4, leftRight)
		require.NoError(t, err)
		assert.Equal(expected2, buf.String())

		paths, err = pathsFromDiff(s3, s4, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths2, paths)
	}

	tf(true)
	tf(false)
}

// This function tests stop functionality in PrintDiff and Diff.
func TestNomsDiffPrintStop(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()
	initmaps(vs)

	expected1 := `(root) {
-   "five"
`

	expected2 := `(root) {
-   map {  // 4 items
`

	s1 := createSet(vs, "one", "three", "five", "seven", "nine")
	s2 := createSet(vs, "one", "three", "five-diff", "seven", "nine")
	s3 := createSet(vs, mm1, mm2, mm3, mm4)
	s4 := createSet(vs, mm1, mm2, mm3x, mm4)

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
	vs := newTestValueStore()
	defer vs.Close()
	initmaps(vs)

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

	s1 := createStruct(vs, "TestData",
		"field1", "field1-data",
		"field2", "field2-data",
		"field3", "field3-data",
	)
	s2 := createStruct(vs, "TestData",
		"field2", "field2-data",
		"field3", "field3-data-diff",
		"field4", "field4-data",
	)

	m1 := createMap(vs, "one", 1, "two", 2, "three", s1, "four", "four")
	m2 := createMap(vs, "one", 1, "two", 2, "three", s2, "four", "four-diff")

	s3 := createStruct(vs, "", "one", 1, "two", 2, "three", s1, "four", "four")
	s4 := createStruct(vs, "", "one", 1, "two", 2, "three", s2, "four", "four-diff")

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		PrintDiff(context.Background(), buf, m1, m2, leftRight)
		assert.Equal(expected1, buf.String())

		paths, err := pathsFromDiff(m1, m2, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths1, paths)

		buf = &bytes.Buffer{}
		err = PrintDiff(context.Background(), buf, s3, s4, leftRight)
		require.NoError(t, err)
		assert.Equal(expected2, buf.String())

		paths, err = pathsFromDiff(s3, s4, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths2, paths)
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintMapWithStructKeys(t *testing.T) {
	a := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()
	initmaps(vs)

	k1 := createStruct(vs, "TestKey", "name", "n1", "label", "l1")

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

	m1, err := types.NewMap(context.Background(), vs, k1, types.Bool(true))
	require.NoError(t, err)
	m2, err := types.NewMap(context.Background(), vs, k1, types.Bool(false))
	require.NoError(t, err)
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
	vs := newTestValueStore()
	defer vs.Close()
	initmaps(vs)

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

	l1 := createList(vs, 1, 2, 3, 4, 44, 5, 6)
	l2 := createList(vs, 1, 22, 3, 4, 5, 6)

	expected2 := `(root) {
+   "seven"
  }
`
	expectedPaths2 := []string{
		`[6]`,
	}

	l3 := createList(vs, "one", "two", "three", "four", "five", "six")
	l4 := createList(vs, "one", "two", "three", "four", "five", "six", "seven")

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

	l5 := createList(vs, mm1, mm2, mm3, mm4)
	l6 := createList(vs, mm1, mm2, mm3x, mm4)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		err := PrintDiff(context.Background(), buf, l1, l2, leftRight)
		require.NoError(t, err)
		assert.Equal(expected1, buf.String())

		paths, err := pathsFromDiff(l1, l2, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths1, paths)

		buf = &bytes.Buffer{}
		err = PrintDiff(context.Background(), buf, l3, l4, leftRight)
		require.NoError(t, err)
		assert.Equal(expected2, buf.String())

		paths, err = pathsFromDiff(l3, l4, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths2, paths)

		buf = &bytes.Buffer{}
		err = PrintDiff(context.Background(), buf, l5, l6, leftRight)
		require.NoError(t, err)
		assert.Equal(expected3, buf.String())

		paths, err = pathsFromDiff(l5, l6, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths3, paths)
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintBlob(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()
	initmaps(vs)

	expected := "-   Blob (2.0 kB)\n+   Blob (11 B)\n"
	expectedPaths1 := []string{``}
	b1, err := types.NewBlob(context.Background(), vs, strings.NewReader(strings.Repeat("x", 2*1024)))
	require.NoError(t, err)
	b2, err := types.NewBlob(context.Background(), vs, strings.NewReader("Hello World"))
	require.NoError(t, err)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		err = PrintDiff(context.Background(), buf, b1, b2, leftRight)
		require.NoError(t, err)
		assert.Equal(expected, buf.String())

		paths, err := pathsFromDiff(b1, b2, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths1, paths)
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintType(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()
	initmaps(vs)

	expected1 := "-   List<Float>\n+   List<String>\n"
	expectedPaths1 := []string{""}
	t1, err := types.MakeListType(types.PrimitiveTypeMap[types.FloatKind])
	require.NoError(t, err)
	t2, err := types.MakeListType(types.PrimitiveTypeMap[types.StringKind])
	require.NoError(t, err)

	expected2 := "-   List<Float>\n+   Set<String>\n"
	expectedPaths2 := []string{``}
	t3, err := types.MakeListType(types.PrimitiveTypeMap[types.FloatKind])
	require.NoError(t, err)
	t4, err := types.MakeSetType(types.PrimitiveTypeMap[types.StringKind])
	require.NoError(t, err)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		err = PrintDiff(context.Background(), buf, t1, t2, leftRight)
		require.NoError(t, err)
		assert.Equal(expected1, buf.String())

		paths, err := pathsFromDiff(t1, t2, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths1, paths)

		buf = &bytes.Buffer{}
		err = PrintDiff(context.Background(), buf, t3, t4, leftRight)
		require.NoError(t, err)
		assert.Equal(expected2, buf.String())

		paths, err = pathsFromDiff(t3, t4, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths2, paths)
	}

	tf(true)
	tf(false)
}

func TestNomsDiffPrintRef(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()
	initmaps(vs)

	expected := "-   #fckcbt7nk5jl4arco2dk7r9nj7abb6ci\n+   #i7d3u5gekm48ot419t2cot6cnl7ltcah\n"
	expectedPaths1 := []string{``}
	l1 := createList(vs, 1)
	l2 := createList(vs, 2)
	r1, err := types.NewRef(l1, vs.Format())
	require.NoError(t, err)
	r2, err := types.NewRef(l2, vs.Format())
	require.NoError(t, err)

	tf := func(leftRight bool) {
		buf := &bytes.Buffer{}
		err := PrintDiff(context.Background(), buf, r1, r2, leftRight)
		require.NoError(t, err)
		test.EqualsIgnoreHashes(t, expected, buf.String())

		paths, err := pathsFromDiff(r1, r2, leftRight)
		require.NoError(t, err)
		assert.Equal(expectedPaths1, paths)
	}

	tf(true)
	tf(false)
}
