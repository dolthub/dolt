// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/diff"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

type orig struct {
	AValue string
}

type replaced struct {
	AValue string
}

type origstruct struct {
	F1 map[string]interface{}
	F2 map[string]interface{}
	F3 orig
	F4 orig
	F5 string
}

var (
	origType     = mustMarshal(orig{"v1"}).Type()
	replacedType = mustMarshal(replaced{"v1"}).Type()
)

func shouldUpdateCb(p types.Path, root, parent, v types.Value) (res bool) {
	return v != nil && v.Type().Kind() == types.StructKind && v.Type().Desc.(types.StructDesc).Name == "Orig"
}

func transformCb(dif diff.Difference) diff.Difference {
	s1 := string(dif.OldValue.(types.Struct).Get("aValue").(types.String))
	nv, _ := marshal.Marshal(replaced{s1})
	return diff.Difference{Path: dif.Path, ChangeType: types.DiffChangeModified, OldValue: dif.OldValue, NewValue: nv}
}

func getPaths(vr types.ValueReader, a1 types.Value, typ *types.Type) ([]types.Path, []types.Path) {
	paths := []types.Path{}
	typedPaths := []types.Path{}
	TreeWalk(vr, nil, a1, func(p types.Path, parent, v types.Value) bool {
		paths = append(paths, p)
		if types.IsSubtype(v.Type(), typ) {
			typedPaths = append(typedPaths, p)
		}
		return false
	})
	return paths, typedPaths

}

func checkParallellGraphs(assert *assert.Assertions, a, b types.Value) {
	vs := types.NewTestValueStore()
	paths1, typedPaths1 := getPaths(vs, a, origType)
	paths2, typedPaths2 := getPaths(vs, b, replacedType)
	assert.Equal(len(paths1), len(paths2))
	assert.Equal(len(typedPaths1), len(typedPaths2))

	for i, p := range paths1 {
		assert.True(p.Equals(paths2[i]), "p1: %s != p2: %s", p, paths2[i])
	}

	for i, p := range typedPaths1 {
		assert.True(p.Equals(typedPaths2[i]), "p1: %s != p2: %s", p, paths2[i])
	}
}

func updateTest(assert *assert.Assertions, a1, a2 types.Value) {
	vs := types.NewTestValueStore()
	b1 := Update(vs, a1, shouldUpdateCb, transformCb, 1)
	checkParallellGraphs(assert, a1, b1)
	b2 := IncrementalUpdate(vs, a2, a1, b1, shouldUpdateCb, transformCb, 1)
	checkParallellGraphs(assert, a2, b2)
}

func updateSetTest(assert *assert.Assertions, a1, a2 types.Value) {
	vs := types.NewTestValueStore()
	b1 := Update(vs, a1, shouldUpdateCb, transformCb, 1)
	paths1, typedPaths1 := getPaths(vs, a1, origType)
	paths2, typedPaths2 := getPaths(vs, b1, replacedType)
	assert.Equal(len(paths1), len(paths2))
	assert.Equal(len(typedPaths1), len(typedPaths2))

	b2 := IncrementalUpdate(vs, a2, a1, b1, shouldUpdateCb, transformCb, 1)
	paths1, typedPaths1 = getPaths(vs, a2, origType)
	paths2, typedPaths2 = getPaths(vs, b2, replacedType)
	assert.Equal(len(paths1), len(paths2))
	assert.Equal(len(typedPaths1), len(typedPaths2))
}

func TestUpdateList(t *testing.T) {
	assert := assert.New(t)

	vs := types.NewTestValueStore()
	defer vs.Close()

	a1 := mustMarshal(map[string][]interface{}{
		"l1": []interface{}{"five", "ten", "fifteen"},
		"l2": []interface{}{orig{"o1"}, "two", "three", "four"},
		"l3": []interface{}{"one", orig{"o1"}, "three", "four"},
		"l4": []interface{}{"one", "two", "three", orig{"o1"}},
		"l5": []interface{}{orig{"o1"}, orig{"o1"}, orig{"o1"}, orig{"o1"}},
	})

	a2 := mustMarshal(map[string][]interface{}{
		"l1": []interface{}{"one", "two", "five", "eight", "eleven", "sixteen"},
		"l2": []interface{}{"two", "three", "four", orig{"o2"}, orig{"o3"}},
		"l3": []interface{}{"one", orig{"o2"}, "three", "four"},
		"l4": []interface{}{"one", "two", "three", "xyxyxy"},
		"l5": []interface{}{orig{"o2"}, orig{"o1"}, orig{"o1"}, orig{"o1"}},
	})

	updateTest(assert, a1, a2)
}

func TestUpdateSet(t *testing.T) {
	assert := assert.New(t)

	a1 := types.NewMap(
		ts("s1"), types.NewSet(ts("one"), ts("two"), mustMarshal(orig{"s1"}), mustMarshal(orig{"s2"})),
		ts("s2"), types.NewSet(
			types.NewMap(
				ts("k11"), ts("v11"),
				ts("k12"), mustMarshal(orig{"s3"}),
			),
			types.NewMap(
				ts("k21"), ts("v21"),
				ts("k22"), mustMarshal(orig{"v4"}),
			),
		),
	)

	a2 := types.NewMap(
		ts("s1"), types.NewSet(ts("one"), ts("two"), mustMarshal(orig{"s44"}), mustMarshal(orig{"s55"})),
		ts("s2"), types.NewSet(
			types.NewMap(
				ts("k11"), ts("v11"),
				ts("k12"), mustMarshal(orig{"s3"}),
			),
			types.NewMap(
				ts("k21"), ts("v21"),
				ts("k22"), ts("changed"),
			),
		),
	)

	updateSetTest(assert, a1, a2)
}

func TestUpdateMap(t *testing.T) {
	assert := assert.New(t)

	a1 := mustMarshal(map[string]orig{
		"o1": orig{"o1"},
		"o2": orig{"o2"},
	})

	a2 := mustMarshal(map[string]interface{}{
		"o2": orig{"o2"},
		"s1": "new field",
		"o3": orig{"o3"},
	})

	updateTest(assert, a1, a2)
}

func TestUpdateMapMixed(t *testing.T) {
	assert := assert.New(t)

	a1 := mustMarshal(map[string][]interface{}{
		"l1": {"one", "two", orig{"o1"}},
		"l2": {"one", "two", orig{"o2"}},
	})

	a2 := mustMarshal(map[string][]interface{}{
		"l1": {"one", "two", orig{"o11"}, orig{"o12"}},
		"l2": {"one"},
	})
	updateTest(assert, a1, a2)
}

func TestUpdateMapNonPrimitives(t *testing.T) {
	assert := assert.New(t)

	a1 := mustMarshal(map[string][]interface{}{
		"l1": {orig{"o11"}, orig{"o12"}, orig{"o13"}},
		"l2": {orig{"o21"}, orig{"o12"}, orig{"o23"}},
	})

	a2 := mustMarshal(map[string][]interface{}{
		"l1": {orig{"o11"}, orig{"o14"}},
		"l2": {orig{"o21"}, orig{"o222"}, orig{"o23"}},
	})
	updateTest(assert, a1, a2)
}

func TestUpdateStruct(t *testing.T) {
	assert := assert.New(t)

	a1 := mustMarshal(map[string]origstruct{
		"t1": origstruct{
			F1: map[string]interface{}{"o1": orig{"o1"}, "two": 2},
			F2: map[string]interface{}{"o2": orig{"o2"}},
			F3: orig{"o3"},
			F4: orig{"o4"},
			F5: "field 5",
		},
		"t2": origstruct{
			F1: map[string]interface{}{"one": 1, "two": 2},
			F2: map[string]interface{}{"o2": orig{"o2"}},
			F3: orig{"o23"},
			F4: orig{"o24"},
			F5: "field 25",
		},
	}).(types.Map)

	a2 := mustMarshal(map[string]origstruct{
		"t1": origstruct{
			F1: map[string]interface{}{"two": 2},
			F2: map[string]interface{}{"o1": orig{"o1"}, "o2": orig{"o2"}},
			F3: orig{"o33"},
			F4: orig{"o4"},
			F5: "field 55",
		},
		"t2": origstruct{
			F1: map[string]interface{}{"one": 1, "two": 2},
			F2: map[string]interface{}{"o2": orig{"o2"}, "o3": orig{"o3"}},
			F3: orig{"o233"},
			F4: orig{"o24"},
			F5: "field 25",
		},
	}).(types.Map)

	// add a field
	a3 := types.NewMap(
		ts("t1"), a2.Get(ts("t1")).(types.Struct).Set("x1", mustMarshal(orig{"x1"})),
		ts("t2"), a2.Get(ts("t2")),
	)

	updateTest(assert, a1, a2)
	updateTest(assert, a2, a3)
}

func ts(s1 string) types.String {
	return types.String(s1)
}
