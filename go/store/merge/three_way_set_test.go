// Copyright 2019 Liquidata, Inc.
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

package merge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/liquidata-inc/dolt/go/store/types"
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
			val = types.NewSet(context.Background(), s.vs, keyValues...)
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
	strRef := s.vs.WriteValue(context.Background(), types.NewStruct(types.Format_7_18, "Foo", types.StructData{"life": types.Float(42)}))

	m := items{s.vs.WriteValue(context.Background(), s.create(flatA)), s.vs.WriteValue(context.Background(), s.create(flatB))}
	ma := items{"r1", s.vs.WriteValue(context.Background(), s.create(flatA))}
	mb := items{"r1", strRef, s.vs.WriteValue(context.Background(), s.create(flatA))}
	mMerged := items{"r1", strRef, s.vs.WriteValue(context.Background(), s.create(flatA))}

	s.tryThreeWayMerge(ma, mb, m, mMerged)
	s.tryThreeWayMerge(mb, ma, m, mMerged)
}

func (s *ThreeWaySetMergeSuite) TestThreeWayMerge_ImmediateConflict() {
	s.tryThreeWayConflict(types.NewMap(context.Background(), s.vs), s.create(ss1b), s.create(ss1), "Cannot merge Map<> with "+s.typeStr)
	s.tryThreeWayConflict(s.create(ss1b), types.NewMap(context.Background(), s.vs), s.create(ss1), "Cannot merge "+s.typeStr)
}
