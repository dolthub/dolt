// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/suite"
)

func TestThreeWaySetMerge(t *testing.T) {
	suite.Run(t, &ThreeWaySetMergeSuite{})
}

type items []interface{}

func (kv items) items() []interface{} {
	return kv
}

type ThreeWaySetMergeSuite struct {
	ThreeWayMergeSuite
}

func (s *ThreeWaySetMergeSuite) SetupSuite() {
	s.create = func(i seq) (val types.Value) {
		if i != nil {
			keyValues := valsToTypesValues(s.create, i.items()...)
			val = types.NewSet(s.vs, keyValues...)
		}
		return
	}
	s.typeStr = "Set"
}

var (
	flat  = items{"a1", "a2", "a3", "a4"}
	flatA = items{"a1", "a2", "a5", "a6"}
	flatB = items{"a1", "a4", "a7", "a5"}
	flatM = items{"a1", "a5", "a6", "a7"}

	ss1       = items{}
	ss1a      = items{"k1", flatA, items{"a", 0}}
	ss1b      = items{"k1", items{"a", 0}, flatB}
	ss1Merged = items{"k1", items{"a", 0}, flatA, flatB}
)

func (s *ThreeWaySetMergeSuite) TestThreeWayMerge_DoNothing() {
	s.tryThreeWayMerge(nil, nil, flat, flat)
}

func (s *ThreeWaySetMergeSuite) TestThreeWayMerge_Primitives() {
	s.tryThreeWayMerge(flatA, flatB, flat, flatM)
	s.tryThreeWayMerge(flatB, flatA, flat, flatM)
}

func (s *ThreeWaySetMergeSuite) TestThreeWayMerge_HandleEmpty() {
	s.tryThreeWayMerge(ss1a, ss1b, ss1, ss1Merged)
	s.tryThreeWayMerge(ss1b, ss1a, ss1, ss1Merged)
}

func (s *ThreeWaySetMergeSuite) TestThreeWayMerge_HandleNil() {
	s.tryThreeWayMerge(ss1a, ss1b, nil, ss1Merged)
	s.tryThreeWayMerge(ss1b, ss1a, nil, ss1Merged)
}

func (s *ThreeWaySetMergeSuite) TestThreeWayMerge_Refs() {
	strRef := s.vs.WriteValue(types.NewStruct("Foo", types.StructData{"life": types.Float(42)}))

	m := items{s.vs.WriteValue(s.create(flatA)), s.vs.WriteValue(s.create(flatB))}
	ma := items{"r1", s.vs.WriteValue(s.create(flatA))}
	mb := items{"r1", strRef, s.vs.WriteValue(s.create(flatA))}
	mMerged := items{"r1", strRef, s.vs.WriteValue(s.create(flatA))}

	s.tryThreeWayMerge(ma, mb, m, mMerged)
	s.tryThreeWayMerge(mb, ma, m, mMerged)
}

func (s *ThreeWaySetMergeSuite) TestThreeWayMerge_ImmediateConflict() {
	s.tryThreeWayConflict(types.NewMap(s.vs), s.create(ss1b), s.create(ss1), "Cannot merge Map<> with "+s.typeStr)
	s.tryThreeWayConflict(s.create(ss1b), types.NewMap(s.vs), s.create(ss1), "Cannot merge "+s.typeStr)
}
