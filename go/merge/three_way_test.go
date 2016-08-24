// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/suite"
)

func TestThreeWayMapMerge(t *testing.T) {
	suite.Run(t, &ThreeWayMapMergeSuite{})
}

func TestThreeWayStructMerge(t *testing.T) {
	suite.Run(t, &ThreeWayStructMergeSuite{})
}

type kvs []interface{}

func (kv kvs) remove(k interface{}) kvs {
	out := make(kvs, 0, len(kv))
	for i := 0; i < len(kv); i++ {
		if kv[i] == k {
			i++ // skip kv[i] and kv[i+1]
			continue
		}
		out = append(out, kv[i])
	}
	return out
}

func (kv kvs) set(k, v interface{}) kvs {
	out := make(kvs, len(kv))
	for i := 0; i < len(kv); i++ {
		out[i] = kv[i]
		if kv[i] == k {
			i++
			out[i] = v
		}
	}
	return out
}

var (
	aa1      = kvs{"a1", "a-one", "a2", "a-two", "a3", "a-three", "a4", "a-four"}
	aa1a     = kvs{"a1", "a-one", "a2", "a-two", "a3", "a-three-diff", "a4", "a-four", "a6", "a-six"}
	aa1b     = kvs{"a1", "a-one", "a3", "a-three-diff", "a4", "a-four", "a5", "a-five"}
	aaMerged = kvs{"a1", "a-one", "a3", "a-three-diff", "a4", "a-four", "a5", "a-five", "a6", "a-six"}

	mm1       = kvs{}
	mm1a      = kvs{"k1", kvs{"a", 0}}
	mm1b      = kvs{"k1", kvs{"b", 1}}
	mm1Merged = kvs{"k1", kvs{"a", 0, "b", 1}}

	mm2       = kvs{"k2", aa1, "k3", "k-three"}
	mm2a      = kvs{"k1", kvs{"a", 0}, "k2", aa1a, "k3", "k-three", "k4", "k-four"}
	mm2b      = kvs{"k1", kvs{"b", 1}, "k2", aa1b}
	mm2Merged = kvs{"k1", kvs{"a", 0, "b", 1}, "k2", aaMerged, "k4", "k-four"}
)

type ThreeWayMergeSuite struct {
	suite.Suite
	create  func(kvs) types.Value
	typeStr string
}

type ThreeWayMapMergeSuite struct {
	ThreeWayMergeSuite
}

func (s *ThreeWayMapMergeSuite) SetupSuite() {
	s.create = func(kv kvs) (val types.Value) {
		if kv != nil {
			keyValues := valsToTypesValues(s.create, kv...)
			val = types.NewMap(keyValues...)
		}
		return
	}
	s.typeStr = "Map"
}

type ThreeWayStructMergeSuite struct {
	ThreeWayMergeSuite
}

func (s *ThreeWayStructMergeSuite) SetupSuite() {
	s.create = func(kv kvs) (val types.Value) {
		if kv != nil {
			fields := types.StructData{}
			for i := 0; i < len(kv); i += 2 {
				fields[kv[i].(string)] = valToTypesValue(s.create, kv[i+1])
			}
			val = types.NewStruct("TestStruct", fields)
		}
		return
	}
	s.typeStr = "struct"
}

func (s *ThreeWayMergeSuite) tryThreeWayMerge(a, b, p, exp kvs, vs types.ValueReadWriter) {
	merged, err := ThreeWay(s.create(a), s.create(b), s.create(p), vs)
	if s.NoError(err) {
		expected := s.create(exp)
		s.True(expected.Equals(merged), "%s != %s", types.EncodedValue(expected), types.EncodedValue(merged))
	}
}

func (s *ThreeWayMergeSuite) tryThreeWayConflict(a, b, p types.Value, contained string) {
	_, err := ThreeWay(a, b, p, nil)
	if s.Error(err) {
		s.Contains(err.Error(), contained)
	}
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_DoNothing() {
	s.tryThreeWayMerge(nil, nil, aa1, aa1, nil)
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_NoRecursion() {
	s.tryThreeWayMerge(aa1a, aa1b, aa1, aaMerged, nil)
	s.tryThreeWayMerge(aa1b, aa1a, aa1, aaMerged, nil)
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_RecursiveCreate() {
	s.tryThreeWayMerge(mm1a, mm1b, mm1, mm1Merged, nil)
	s.tryThreeWayMerge(mm1b, mm1a, mm1, mm1Merged, nil)
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_RecursiveCreateNil() {
	s.tryThreeWayMerge(mm1a, mm1b, nil, mm1Merged, nil)
	s.tryThreeWayMerge(mm1b, mm1a, nil, mm1Merged, nil)
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_RecursiveMerge() {
	s.tryThreeWayMerge(mm2a, mm2b, mm2, mm2Merged, nil)
	s.tryThreeWayMerge(mm2b, mm2a, mm2, mm2Merged, nil)
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_RefMerge() {
	vs := types.NewTestValueStore()

	strRef := vs.WriteValue(types.NewStruct("Foo", types.StructData{"life": types.Number(42)}))

	m := kvs{"r2", vs.WriteValue(s.create(aa1))}
	ma := kvs{"r1", strRef, "r2", vs.WriteValue(s.create(aa1a))}
	mb := kvs{"r1", strRef, "r2", vs.WriteValue(s.create(aa1b))}
	mMerged := kvs{"r1", strRef, "r2", vs.WriteValue(s.create(aaMerged))}
	vs.Flush()

	s.tryThreeWayMerge(ma, mb, m, mMerged, vs)
	s.tryThreeWayMerge(mb, ma, m, mMerged, vs)
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_RecursiveMultiLevelMerge() {
	vs := types.NewTestValueStore()

	m := kvs{"mm1", mm1, "mm2", vs.WriteValue(s.create(mm2))}
	ma := kvs{"mm1", mm1a, "mm2", vs.WriteValue(s.create(mm2a))}
	mb := kvs{"mm1", mm1b, "mm2", vs.WriteValue(s.create(mm2b))}
	mMerged := kvs{"mm1", mm1Merged, "mm2", vs.WriteValue(s.create(mm2Merged))}
	vs.Flush()

	s.tryThreeWayMerge(ma, mb, m, mMerged, vs)
	s.tryThreeWayMerge(mb, ma, m, mMerged, vs)
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_NilConflict() {
	s.tryThreeWayConflict(nil, s.create(mm2b), s.create(mm2), "Cannot merge nil Value with")
	s.tryThreeWayConflict(s.create(mm2a), nil, s.create(mm2), "with nil value.")
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_ImmediateConflict() {
	s.tryThreeWayConflict(types.NewSet(), s.create(mm2b), s.create(mm2), "Cannot merge Set<> with "+s.typeStr)
	s.tryThreeWayConflict(s.create(mm2b), types.NewSet(), s.create(mm2), "Cannot merge "+s.typeStr)
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_NestedConflict() {
	a := mm2a.set("k2", types.NewSet())
	s.tryThreeWayConflict(s.create(a), s.create(mm2b), s.create(mm2), types.EncodedValue(types.NewSet()))
	s.tryThreeWayConflict(s.create(a), s.create(mm2b), s.create(mm2), types.EncodedValue(s.create(aa1b)))
}

func (s *ThreeWayMergeSuite) TestThreeWayMergeMap_NestedConflictingOperation() {
	a := mm2a.remove("k2")
	s.tryThreeWayConflict(s.create(a), s.create(mm2b), s.create(mm2), `removed "k2"`)
	s.tryThreeWayConflict(s.create(a), s.create(mm2b), s.create(mm2), `modded "k2"`)
}

func valsToTypesValues(f func(kvs) types.Value, kv ...interface{}) []types.Value {
	keyValues := []types.Value{}
	for _, e := range kv {
		v := valToTypesValue(f, e)
		keyValues = append(keyValues, v)
	}
	return keyValues
}

func valToTypesValue(f func(kvs) types.Value, v interface{}) types.Value {
	var v1 types.Value
	switch t := v.(type) {
	case string:
		v1 = types.String(t)
	case int:
		v1 = types.Number(t)
	case kvs:
		v1 = f(t)
	case types.Value:
		v1 = t
	}
	return v1
}
