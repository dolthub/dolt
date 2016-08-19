// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

var (
	aa1      = createMap("a1", "a-one", "a2", "a-two", "a3", "a-three", "a4", "a-four")
	aa1a     = createMap("a1", "a-one", "a2", "a-two", "a3", "a-three-diff", "a4", "a-four", "a6", "a-six")
	aa1b     = createMap("a1", "a-one", "a3", "a-three-diff", "a4", "a-four", "a5", "a-five")
	aaMerged = createMap("a1", "a-one", "a3", "a-three-diff", "a4", "a-four", "a5", "a-five", "a6", "a-six")

	mm1       = createMap()
	mm1a      = createMap("k1", createMap(0, "a"))
	mm1b      = createMap("k1", createMap(1, "b"))
	mm1Merged = createMap("k1", createMap(0, "a", 1, "b"))

	mm2       = createMap("k2", aa1, "k3", "k-three")
	mm2a      = createMap("k1", createMap(0, "a"), "k2", aa1a, "k3", "k-three", "k4", "k-four")
	mm2b      = createMap("k1", createMap(1, "b"), "k2", aa1b)
	mm2Merged = createMap("k1", createMap(0, "a", 1, "b"), "k2", aaMerged, "k4", "k-four")
)

func tryThreeWayMerge(t *testing.T, a, b, p, expected types.Value, vs types.ValueReadWriter) {
	merged, err := ThreeWay(a, b, p, vs)
	if assert.NoError(t, err) {
		assert.True(t, expected.Equals(merged))
	}
}

func TestThreeWayMergeMap_DoNothing(t *testing.T) {
	tryThreeWayMerge(t, nil, nil, aa1, aa1, nil)
}

func TestThreeWayMergeMap_NoRecursion(t *testing.T) {
	tryThreeWayMerge(t, aa1a, aa1b, aa1, aaMerged, nil)
	tryThreeWayMerge(t, aa1b, aa1a, aa1, aaMerged, nil)
}

func TestThreeWayMergeMap_RecursiveCreate(t *testing.T) {
	tryThreeWayMerge(t, mm1a, mm1b, mm1, mm1Merged, nil)
	tryThreeWayMerge(t, mm1b, mm1a, mm1, mm1Merged, nil)
}

func TestThreeWayMergeMap_RecursiveCreateNil(t *testing.T) {
	tryThreeWayMerge(t, mm1a, mm1b, nil, mm1Merged, nil)
	tryThreeWayMerge(t, mm1b, mm1a, nil, mm1Merged, nil)
}

func TestThreeWayMergeMap_RecursiveMerge(t *testing.T) {
	tryThreeWayMerge(t, mm2a, mm2b, mm2, mm2Merged, nil)
	tryThreeWayMerge(t, mm2b, mm2a, mm2, mm2Merged, nil)
}

func TestThreeWayMergeMap_RefMerge(t *testing.T) {
	vs := types.NewTestValueStore()

	strRef := vs.WriteValue(types.NewStruct("Foo", types.StructData{"life": types.Number(42)}))

	m := createMap("r2", vs.WriteValue(aa1))
	ma := createMap("r1", strRef, "r2", vs.WriteValue(aa1a))
	mb := createMap("r1", strRef, "r2", vs.WriteValue(aa1b))
	mMerged := createMap("r1", strRef, "r2", vs.WriteValue(aaMerged))
	vs.Flush()

	tryThreeWayMerge(t, ma, mb, m, mMerged, vs)
	tryThreeWayMerge(t, mb, ma, m, mMerged, vs)
}

func TestThreeWayMergeMap_RecursiveMultiLevelMerge(t *testing.T) {
	vs := types.NewTestValueStore()

	m := createMap("mm1", mm1, "mm2", vs.WriteValue(mm2))
	ma := createMap("mm1", mm1a, "mm2", vs.WriteValue(mm2a))
	mb := createMap("mm1", mm1b, "mm2", vs.WriteValue(mm2b))
	mMerged := createMap("mm1", mm1Merged, "mm2", vs.WriteValue(mm2Merged))
	vs.Flush()

	tryThreeWayMerge(t, ma, mb, m, mMerged, vs)
	tryThreeWayMerge(t, mb, ma, m, mMerged, vs)
}

func tryThreeWayConflict(t *testing.T, a, b, p types.Value, contained string) {
	_, err := ThreeWay(a, b, p, nil)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), contained)
	}
}

func TestThreeWayMergeMap_NilConflict(t *testing.T) {
	tryThreeWayConflict(t, nil, mm2b, mm2, "Cannot merge nil Value with")
	tryThreeWayConflict(t, mm2a, nil, mm2, "with nil value.")
}

func TestThreeWayMergeMap_ImmediateConflict(t *testing.T) {
	tryThreeWayConflict(t, types.NewSet(), mm2b, mm2, "Cannot merge Set<> with Map")
	tryThreeWayConflict(t, mm2b, types.NewSet(), mm2, "Cannot merge Map")
}

func TestThreeWayMergeMap_NestedConflict(t *testing.T) {
	tryThreeWayConflict(t, mm2a.Set(types.String("k2"), types.NewSet()), mm2b, mm2, types.EncodedValue(types.NewSet()))
	tryThreeWayConflict(t, mm2a.Set(types.String("k2"), types.NewSet()), mm2b, mm2, types.EncodedValue(aa1b))
}

func TestThreeWayMergeMap_NestedConflictingOperation(t *testing.T) {
	key := types.String("k2")
	tryThreeWayConflict(t, mm2a.Remove(key), mm2b, mm2, "removed "+types.EncodedValue(key))
	tryThreeWayConflict(t, mm2a.Remove(key), mm2b, mm2, "modded "+types.EncodedValue(key))
}

func createMap(kv ...interface{}) types.Map {
	keyValues := valsToTypesValues(kv...)
	return types.NewMap(keyValues...)
}

func valsToTypesValues(kv ...interface{}) []types.Value {
	keyValues := []types.Value{}
	for _, e := range kv {
		v := valToTypesValue(e)
		keyValues = append(keyValues, v)
	}
	return keyValues
}

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
