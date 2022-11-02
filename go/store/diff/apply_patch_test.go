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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/types"
)

func mustValue(v types.Value, err error) types.Value {
	d.PanicIfError(err)
	return v
}

func mustGetValue(v types.Value, found bool, err error) types.Value {
	d.PanicIfError(err)
	d.PanicIfFalse(found)
	return v
}

func TestCommonPrefixCount(t *testing.T) {
	assert := assert.New(t)

	testCases := [][]interface{}{
		{".value[#94a2oa20oka0jdv5lha03vuvvumul1vb].sizes[#316j9oc39b09fbc2qf3klenm6p1o1d7h]", 0},
		{".value[#94a2oa20oka0jdv5lha03vuvvumul1vb].sizes[#77eavttned7llu1pkvhaei9a9qgcagir]", 3},
		{".value[#94a2oa20oka0jdv5lha03vuvvumul1vb].sizes[#hboaq9581drq4g9jf62d3s06al3us49s]", 3},
		{".value[#94a2oa20oka0jdv5lha03vuvvumul1vb].sizes[#l0hpa7sbr7qutrcfn5173kar4j2847m1]", 3},
		{".value[#9vj5m3049mav94bttcujhgfdfqcavsbn].sizes[#33f6tb4h8agh57s2bqlmi9vbhlkbtmct]", 1},
		{".value[#9vj5m3049mav94bttcujhgfdfqcavsbn].sizes[#a43ne9a8kotcqph4up5pqqdmr1e1qcsl]", 3},
		{".value[#9vj5m3049mav94bttcujhgfdfqcavsbn].sizes[#ppqg6pem2sb64h2i2ptnh8ckj8gogj9h]", 3},
		{".value[#9vj5m3049mav94bttcujhgfdfqcavsbn].sizes[#s7r2vpnqlk20sd72mg8ijerg9cmauaqo]", 3},
		{".value[#bpspmmlc41pk0r144a7682oah0tmge1e].sizes[#9vuc1gg3c3eude5v3j5deqopjsobe3no]", 1},
		{".value[#bpspmmlc41pk0r144a7682oah0tmge1e].sizes[#qo3gfdsf14v3dh0oer82vn1bg4o8nlsc]", 3},
		{".value[#bpspmmlc41pk0r144a7682oah0tmge1e].sizes[#rlidki5ipbjdofsm2rq3a66v908m5fpl]", 3},
		{".value[#bpspmmlc41pk0r144a7682oah0tmge1e].sizes[#st1n96rh89c2vgo090dt9lknd5ip4kck]", 3},
		{".value[#hjh5hpn55591k0gjvgckc14erli968ao].sizes[#267889uv3mtih6fij3fhio2jiqtl6nho]", 1},
		{".value[#hjh5hpn55591k0gjvgckc14erli968ao].sizes[#7ncb7guoip9e400bm2lcvr0dda29o9jn]", 3},
		{".value[#hjh5hpn55591k0gjvgckc14erli968ao].sizes[#afscb0on7rt8bq6eutup8juusmid7i96]", 3},
		{".value[#hjh5hpn55591k0gjvgckc14erli968ao].sizes[#drqe4lr0vdfdtmvejsjun1l3mfv6ums5]", 3},
	}

	var lastPath types.Path

	for i, tc := range testCases {
		path, expected := tc[0].(string), tc[1].(int)
		p, err := types.ParsePath(path)
		require.NoError(t, err)
		assert.Equal(expected, commonPrefixCount(lastPath, p), "failed for paths[%d]: %s", i, path)
		lastPath = p
	}
}

type testFunc func(parent types.Value) types.Value
type testKey struct {
	X, Y int
}

var (
	vm map[string]types.Value
)

func vfk(keys ...string) []types.Value {
	var values []types.Value
	for _, k := range keys {
		values = append(values, vm[k])
	}
	return values
}

func testValues(vrw types.ValueReadWriter) map[string]types.Value {
	if vm == nil {
		vm = map[string]types.Value{
			"k1":      types.String("k1"),
			"k2":      types.String("k2"),
			"k3":      types.String("k3"),
			"s1":      types.String("string1"),
			"s2":      types.String("string2"),
			"s3":      types.String("string3"),
			"s4":      types.String("string4"),
			"n1":      types.Float(1),
			"n2":      types.Float(2),
			"n3":      types.Float(3.3),
			"n4":      types.Float(4.4),
			"b1":      mustMarshal(true),
			"b2":      mustMarshal(false),
			"l1":      mustMarshal([]string{}),
			"l2":      mustMarshal([]string{"one", "two", "three", "four"}),
			"l3":      mustMarshal([]string{"two", "three", "four", "five"}),
			"l4":      mustMarshal([]string{"two", "three", "four"}),
			"l5":      mustMarshal([]string{"one", "two", "three", "four", "five"}),
			"l6":      mustMarshal([]string{"one", "four"}),
			"struct1": mustValue(types.NewStruct(vrw.Format(), "test1", types.StructData{"f1": types.Float(1), "f2": types.Float(2)})),
			"struct2": mustValue(types.NewStruct(vrw.Format(), "test1", types.StructData{"f1": types.Float(11111), "f2": types.Float(2)})),
			"struct3": mustValue(types.NewStruct(vrw.Format(), "test1", types.StructData{"f1": types.Float(1), "f2": types.Float(2), "f3": types.Float(3)})),
			"struct4": mustValue(types.NewStruct(vrw.Format(), "test1", types.StructData{"f2": types.Float(2)})),
			"m1":      mustMarshal(map[string]int{}),
			"m2":      mustMarshal(map[string]int{"k1": 1, "k2": 2, "k3": 3}),
			"m3":      mustMarshal(map[string]int{"k2": 2, "k3": 3, "k4": 4}),
			"m4":      mustMarshal(map[string]int{"k1": 1, "k3": 3}),
			"m5":      mustMarshal(map[string]int{"k1": 1, "k2": 2222, "k3": 3}),
			"ms1":     mustMarshal(map[testKey]int{{1, 1}: 1, {2, 2}: 2, {3, 3}: 3}),
			"ms2":     mustMarshal(map[testKey]int{{1, 1}: 1, {4, 4}: 4, {5, 5}: 5}),
		}

		vm["mh1"] = mustValue(types.NewMap(context.Background(), vrw, vfk("k1", "struct1", "k2", "l1")...))
		vm["mh2"] = mustValue(types.NewMap(context.Background(), vrw, vfk("k1", "n1", "k2", "l2", "k3", "l3")...))
		vm["set1"] = mustValue(types.NewSet(context.Background(), vrw))
		vm["set2"] = mustValue(types.NewSet(context.Background(), vrw, vfk("s1", "s2")...))
		vm["set3"] = mustValue(types.NewSet(context.Background(), vrw, vfk("s1", "s2", "s3")...))
		vm["set1"] = mustValue(types.NewSet(context.Background(), vrw, vfk("s2")...))
		vm["seth1"] = mustValue(types.NewSet(context.Background(), vrw, vfk("struct1", "struct2", "struct3")...))
		vm["seth2"] = mustValue(types.NewSet(context.Background(), vrw, vfk("struct2", "struct3")...))
		vm["setj3"] = mustValue(types.NewSet(context.Background(), vrw, vfk("struct1")...))
		vm["mk1"] = mustValue(types.NewMap(context.Background(), vrw, vfk("struct1", "s1", "struct2", "s2")...))
		vm["mk2"] = mustValue(types.NewMap(context.Background(), vrw, vfk("struct1", "s3", "struct4", "s4")...))
	}
	return vm
}

func newTestValueStore() *types.ValueStore {
	st := &chunks.TestStorage{}
	return types.NewValueStore(st.NewViewWithDefaultFormat())
}

func getPatch(g1, g2 types.Value) (Patch, error) {
	var derr error
	dChan := make(chan Difference)
	go func() {
		defer close(dChan)
		derr = Diff(context.Background(), g1, g2, dChan, true, nil)
	}()

	patch := Patch{}
	for dif := range dChan {
		patch = append(patch, dif)
	}

	return patch, derr
}

func checkApplyPatch(assert *assert.Assertions, vrw types.ValueReadWriter, g1, expectedG2 types.Value, k1, k2 string) {
	patch, err := getPatch(g1, expectedG2)
	assert.NoError(err)
	g2, err := Apply(context.Background(), vrw.Format(), g1, patch)
	assert.NoError(err)
	assert.True(expectedG2.Equals(g2), "failed to apply diffs for k1: %s and k2: %s", k1, k2)
}

func TestPatches(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	cnt := 0
	for k1, g1 := range testValues(vs) {
		for k2, expectedG2 := range testValues(vs) {
			if k1 != k2 {
				cnt++
				checkApplyPatch(assert, vs, g1, expectedG2, k1, k2)
			}
		}
	}
}

func TestNestedLists(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	ol1 := mustMarshal([]string{"one", "two", "three", "four"})
	nl1 := mustMarshal([]string{"two", "three"})
	ol2 := mustMarshal([]int{2, 3})
	nl2 := mustMarshal([]int{1, 2, 3, 4})
	nl3 := mustMarshal([]bool{true, false, true})
	g1 := mustValue(types.NewList(context.Background(), vs, ol1, ol2))
	g2 := mustValue(types.NewList(context.Background(), vs, nl1, nl2, nl3))
	checkApplyPatch(assert, vs, g1, g2, "g1", "g2")
}

func TestUpdateNode(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	doTest := func(pp types.PathPart, parent, ov, nv, exp types.Value, f testFunc) {
		stack := &patchStack{}
		se := &stackElem{path: []types.PathPart{pp}, pathPart: pp, changeType: types.DiffChangeModified, oldValue: ov, newValue: nv}
		updated, err := stack.updateNode(context.Background(), se, parent)
		require.NoError(t, err)
		testVal := f(updated)
		assert.True(exp.Equals(testVal), "%s != %s", nv, testVal)
	}

	var pp types.PathPart
	oldVal := types.String("Yo")
	newVal := types.String("YooHoo")

	s1, err := types.NewStruct(vs.Format(), "TestStruct", types.StructData{"f1": types.Float(1), "f2": oldVal})
	require.NoError(t, err)
	pp = types.FieldPath{Name: "f2"}
	doTest(pp, s1, oldVal, newVal, newVal, func(parent types.Value) types.Value {
		return mustGetValue(parent.(types.Struct).MaybeGet("f2"))
	})

	l1, err := types.NewList(context.Background(), vs, types.String("one"), oldVal, types.String("three"))
	require.NoError(t, err)
	pp = types.IndexPath{Index: types.Float(1)}
	doTest(pp, l1, oldVal, newVal, newVal, func(parent types.Value) types.Value {
		return mustValue(parent.(types.List).Get(context.Background(), 1))
	})

	m1, err := types.NewMap(context.Background(), vs, types.String("k1"), types.Float(1), types.String("k2"), oldVal)
	require.NoError(t, err)
	pp = types.IndexPath{Index: types.String("k2")}
	doTest(pp, m1, oldVal, newVal, newVal, func(parent types.Value) types.Value {
		return mustGetValue(parent.(types.Map).MaybeGet(context.Background(), types.String("k2")))
	})

	k1, err := types.NewStruct(vs.Format(), "Sizes", types.StructData{"height": types.Float(200), "width": types.Float(300)})
	require.NoError(t, err)
	_, err = vs.WriteValue(context.Background(), k1)
	require.NoError(t, err)
	m1, err = types.NewMap(context.Background(), vs, k1, oldVal)
	require.NoError(t, err)
	h, err := k1.Hash(vs.Format())
	require.NoError(t, err)
	pp = types.HashIndexPath{Hash: h}
	doTest(pp, m1, oldVal, newVal, newVal, func(parent types.Value) types.Value {
		return mustGetValue(parent.(types.Map).MaybeGet(context.Background(), k1))
	})

	set1, err := types.NewSet(context.Background(), vs, oldVal, k1)
	require.NoError(t, err)
	pp = types.IndexPath{Index: oldVal}
	exp, err := types.NewSet(context.Background(), vs, newVal, k1)
	require.NoError(t, err)
	doTest(pp, set1, oldVal, newVal, exp, func(parent types.Value) types.Value {
		return parent
	})

	k2, err := types.NewStruct(vs.Format(), "Sizes", types.StructData{"height": types.Float(300), "width": types.Float(500)})
	require.NoError(t, err)
	set1, err = types.NewSet(context.Background(), vs, oldVal, k1)
	require.NoError(t, err)
	h, err = k1.Hash(vs.Format())
	require.NoError(t, err)
	pp = types.HashIndexPath{Hash: h}
	exp, err = types.NewSet(context.Background(), vs, oldVal, k2)
	require.NoError(t, err)
	doTest(pp, set1, k1, k2, exp, func(parent types.Value) types.Value {
		return parent
	})
}

func checkApplyDiffs(a *assert.Assertions, vrw types.ValueReadWriter, n1, n2 types.Value, leftRight bool) {
	var derr error
	dChan := make(chan Difference)
	go func() {
		defer close(dChan)
		derr = Diff(context.Background(), n1, n2, dChan, leftRight, nil)
	}()

	difs := Patch{}
	for dif := range dChan {
		difs = append(difs, dif)
	}

	a.NoError(derr)

	res, err := Apply(context.Background(), vrw.Format(), n1, difs)
	a.NoError(err)
	a.True(n2.Equals(res))
}

func tryApplyDiff(a *assert.Assertions, vrw types.ValueReadWriter, a1, a2 interface{}) {
	n1 := mustMarshal(a1)
	n2 := mustMarshal(a2)

	checkApplyDiffs(a, vrw, n1, n2, true)
	checkApplyDiffs(a, vrw, n1, n2, false)
	checkApplyDiffs(a, vrw, n2, n1, true)
	checkApplyDiffs(a, vrw, n2, n1, false)
}

func TestUpdateList(t *testing.T) {
	a := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()

	// insert at beginning
	a1 := []interface{}{"five", "ten", "fifteen"}
	a2 := []interface{}{"one", "two", "three", "five", "ten", "fifteen"}
	tryApplyDiff(a, vs, a1, a2)

	// append at end
	a1 = []interface{}{"five", "ten", "fifteen"}
	a2 = []interface{}{"five", "ten", "fifteen", "twenty", "twenty-five"}
	tryApplyDiff(a, vs, a1, a2)

	// insert interleaved
	a1 = []interface{}{"one", "three", "five", "seven"}
	a2 = []interface{}{"one", "two", "three", "four", "five", "six", "seven"}
	tryApplyDiff(a, vs, a1, a2)

	// delete from beginning and append to end
	a1 = []interface{}{"one", "two", "three", "four", "five"}
	a2 = []interface{}{"four", "five", "six", "seven"}
	tryApplyDiff(a, vs, a1, a2)

	// replace entries at beginning
	a1 = []interface{}{"one", "two", "three", "four", "five"}
	a2 = []interface{}{"3.5", "four", "five"}
	tryApplyDiff(a, vs, a1, a2)

	// replace entries at end
	a1 = []interface{}{"one", "two", "three"}
	a2 = []interface{}{"one", "four"}
	tryApplyDiff(a, vs, a1, a2)

	// insert at beginning, replace at end
	a1 = []interface{}{"five", "ten", "fifteen"}
	a2 = []interface{}{"one", "two", "five", "eight", "eleven", "sixteen", "twenty"}
	tryApplyDiff(a, vs, a1, a2)

	// remove everything
	a1 = []interface{}{"five", "ten", "fifteen"}
	a2 = []interface{}{}
	tryApplyDiff(a, vs, a1, a2)
}

func TestUpdateMap(t *testing.T) {
	a := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()

	// insertions, deletions, and replacements
	a1 := map[string]int{"five": 5, "ten": 10, "fifteen": 15, "twenty": 20}
	a2 := map[string]int{"one": 1, "two": 2, "three": 3, "five": 5, "ten": 10, "fifteen": 15, "twenty": 2020}
	tryApplyDiff(a, vs, a1, a2)

	// delete everything
	a1 = map[string]int{"five": 5, "ten": 10, "fifteen": 15, "twenty": 20}
	a2 = map[string]int{}
	tryApplyDiff(a, vs, a1, a2)
}

func TestUpdateStruct(t *testing.T) {
	a := assert.New(t)
	vs := newTestValueStore()
	defer vs.Close()

	a1 := mustValue(types.NewStruct(vs.Format(), "tStruct", types.StructData{
		"f1": types.Float(1),
		"f2": types.String("two"),
		"f3": mustMarshal([]string{"one", "two", "three"}),
	}))
	a2 := mustValue(types.NewStruct(vs.Format(), "tStruct", types.StructData{
		"f1": types.Float(2),
		"f2": types.String("twotwo"),
		"f3": mustMarshal([]interface{}{0, "one", 1, "two", 2, "three", 3}),
	}))
	checkApplyDiffs(a, vs, a1, a2, true)
	checkApplyDiffs(a, vs, a1, a2, false)

	a2 = mustValue(types.NewStruct(vs.Format(), "tStruct", types.StructData{
		"f1": types.Float(2),
		"f2": types.String("two"),
		"f3": mustMarshal([]interface{}{0, "one", 1, "two", 2, "three", 3}),
		"f4": types.Bool(true),
	}))
	checkApplyDiffs(a, vs, a1, a2, true)
	checkApplyDiffs(a, vs, a1, a2, false)
}

func TestUpdateSet(t *testing.T) {
	a := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	a1 := mustValue(types.NewSet(context.Background(), vs, types.Float(1), types.String("two"), mustMarshal([]string{"one", "two", "three"})))
	a2 := mustValue(types.NewSet(context.Background(), vs, types.Float(3), types.String("three"), mustMarshal([]string{"one", "two", "three", "four"})))

	checkApplyDiffs(a, vs, a1, a2, true)
	checkApplyDiffs(a, vs, a1, a2, false)
	checkApplyDiffs(a, vs, a2, a1, true)
	checkApplyDiffs(a, vs, a2, a1, false)
}

func mustMarshal(v interface{}) types.Value {
	vs := newTestValueStore()
	defer vs.Close()

	v1, err := marshal.Marshal(context.Background(), vs, v)
	d.Chk.NoError(err)
	return v1
}
