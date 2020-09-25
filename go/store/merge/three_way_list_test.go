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

	"github.com/dolthub/dolt/go/store/types"
)

func TestThreeWayListMerge(t *testing.T) {
	suite.Run(t, &ThreeWayListMergeSuite{})
}

type ThreeWayListMergeSuite struct {
	ThreeWayMergeSuite
}

func (s *ThreeWayListMergeSuite) SetupSuite() {
	s.create = func(i seq) (types.Value, error) {
		if i != nil {
			items, err := valsToTypesValues(s.create, i.items()...)

			if err != nil {
				return nil, err
			}

			val, err := types.NewList(context.Background(), s.vs, items...)

			if err != nil {
				return nil, err
			}

			return val, nil
		}

		return nil, nil
	}
	s.typeStr = "List"
}

var p = items{"a", "b", "c", "d", "e"}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_DoNothing() {
	s.tryThreeWayMerge(nil, nil, p, p)
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_NoLengthChange() {
	a := items{"a", 1, "c", "d", "e"}
	b := items{"a", "b", "c", 2, "e"}
	m := items{"a", 1, "c", 2, "e"}
	s.tryThreeWayMerge(a, b, p, m)
	s.tryThreeWayMerge(b, a, p, m)
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_HandleEmpty() {
	s.tryThreeWayMerge(p, items{}, items{}, p)
	s.tryThreeWayMerge(items{}, p, items{}, p)
	s.tryThreeWayMerge(p, p, items{}, p)
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_HandleNil() {
	s.tryThreeWayMerge(p, items{}, nil, p)
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_MakeLonger() {
	a := items{"a", 1, 2, "c", "d", "e"}
	b := items{"a", "b", "c", 3, "e"}
	m := items{"a", 1, 2, "c", 3, "e"}
	s.tryThreeWayMerge(a, b, p, m)
	s.tryThreeWayMerge(b, a, p, m)

}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_MakeShorter() {
	a := items{"a", "c", "d", "e"}
	b := items{"a", "b", "c", 3, "e"}
	m := items{"a", "c", 3, "e"}
	s.tryThreeWayMerge(a, b, p, m)
	s.tryThreeWayMerge(b, a, p, m)
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_BothSidesRemove() {
	a := items{"a", "c", "d", "e"}
	b := items{"a", "b", "c", "e"}
	m := items{"a", "c", "e"}
	s.tryThreeWayMerge(a, b, p, m)
	s.tryThreeWayMerge(b, a, p, m)
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_OverlapSameRemoveNoInsert() {
	a := items{"a", "d", "e"}
	b := items{"a", "d", "e"}
	m := items{"a", "d", "e"}
	s.tryThreeWayMerge(a, b, p, m)
	s.tryThreeWayMerge(b, a, p, m)
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_OverlapSameRemoveSameInsert() {
	a := items{"a", 1, 2, 3, "d", "e"}
	b := items{"a", 1, 2, 3, "d", "e"}
	m := items{"a", 1, 2, 3, "d", "e"}
	s.tryThreeWayMerge(a, b, p, m)
	s.tryThreeWayMerge(b, a, p, m)
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_RemoveUpToOtherSideInsertionPoint() {
	a := items{"a", 1, 2, "c", "d", "e"}
	b := items{"a", "b", 3, "c", "d", "e"}
	m := items{"a", 1, 2, 3, "c", "d", "e"}
	s.tryThreeWayMerge(a, b, p, m)
	s.tryThreeWayMerge(b, a, p, m)
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_ConflictingAppends() {
	a := append(p, 1)
	b := append(p, 2)
	s.tryThreeWayConflict(mustValue(s.create(a)), mustValue(s.create(b)), mustValue(s.create(p)), "Overlapping splices: 0 elements removed at 5; adding 1 elements")
	s.tryThreeWayConflict(mustValue(s.create(b)), mustValue(s.create(a)), mustValue(s.create(p)), "Overlapping splices: 0 elements removed at 5; adding 1 elements")
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_OverlappingRemoves() {
	a := p[:4]
	b := p[:3]
	s.tryThreeWayConflict(mustValue(s.create(a)), mustValue(s.create(b)), mustValue(s.create(p)), "Overlapping splices: 1 elements removed at 4")
	s.tryThreeWayConflict(mustValue(s.create(b)), mustValue(s.create(a)), mustValue(s.create(p)), "Overlapping splices: 2 elements removed at 3")
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_SameRemoveAddPrefix() {
	a := items{"a", "b", "c", 1}
	b := items{"a", "b", "c", 1, 2}
	s.tryThreeWayConflict(mustValue(s.create(a)), mustValue(s.create(b)), mustValue(s.create(p)), "Overlapping splices: 2 elements removed at 3; adding 1 elements")
	s.tryThreeWayConflict(mustValue(s.create(b)), mustValue(s.create(a)), mustValue(s.create(p)), "Overlapping splices: 2 elements removed at 3; adding 2 elements")
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_RemoveSupersetAddPrefix() {
	a := items{"a", "b", "c", 1, 2}
	b := items{"a", "b", "c", "d", 1}
	s.tryThreeWayConflict(mustValue(s.create(a)), mustValue(s.create(b)), mustValue(s.create(p)), "Overlapping splices: 2 elements removed at 3; adding 2 elements")
	s.tryThreeWayConflict(mustValue(s.create(b)), mustValue(s.create(a)), mustValue(s.create(p)), "Overlapping splices: 1 elements removed at 4; adding 1 elements")
}

func (s *ThreeWayListMergeSuite) TestThreeWayMerge_RemoveOtherSideInsertionPoint() {
	a := items{"a", "c", "d", "e"}
	b := items{"a", 1, "b", "c", "d", "e"}
	s.tryThreeWayConflict(mustValue(s.create(a)), mustValue(s.create(b)), mustValue(s.create(p)), "Overlapping splices: 1 elements removed at 1; adding 0 elements")
	s.tryThreeWayConflict(mustValue(s.create(b)), mustValue(s.create(a)), mustValue(s.create(p)), "Overlapping splices: 0 elements removed at 1; adding 1 elements")
}
