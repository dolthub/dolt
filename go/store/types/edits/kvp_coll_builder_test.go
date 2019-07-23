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

package edits

import (
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"testing"
)

func TestAddKVP(t *testing.T) {
	builder := NewKVPCollBuilder(2)
	builder.AddKVP(types.KVP{types.Uint(0), types.NullValue})
	builder.AddKVP(types.KVP{types.Uint(1), types.NullValue})
	builder.AddKVP(types.KVP{types.Uint(2), types.NullValue})

	coll := builder.Build()
	itr := coll.Iterator()

	for i := int64(0); i < coll.Size(); i++ {
		kvp := itr.Next()

		if uint(kvp.Key.(types.Uint)) != uint(i) {
			t.Error("enexpected value")
		}
	}
}

func TestMoveRemaining(t *testing.T) {
	sl1 := types.KVPSlice{{types.Uint(0), types.NullValue}, {types.Uint(1), types.NullValue}}
	sl2 := types.KVPSlice{{types.Uint(2), types.NullValue}, {}}
	coll := &KVPCollection{
		2,
		2,
		3,
		[]types.KVPSlice{sl1, sl2[:1]},
		types.Format_7_18,
	}

	builder := NewKVPCollBuilder(2)
	builder.MoveRemaining(coll.Iterator())

	result := builder.Build()
	itr := result.Iterator()

	for i := int64(0); i < result.Size(); i++ {
		kvp := itr.Next()

		if uint(kvp.Key.(types.Uint)) != uint(i) {
			t.Error("enexpected value")
		}
	}
}

func TestAddKVPAndMoveRemaining(t *testing.T) {
	sl := types.KVPSlice{{types.Uint(1), types.NullValue}, {types.Uint(2), types.NullValue}}
	coll := NewKVPCollection(types.Format_7_18, sl)

	builder := NewKVPCollBuilder(2)
	builder.AddKVP(types.KVP{types.Uint(0), types.NullValue})
	builder.MoveRemaining(coll.Iterator())

	result := builder.Build()
	itr := result.Iterator()

	for i := int64(0); i < result.Size(); i++ {
		kvp := itr.Next()

		if uint(kvp.Key.(types.Uint)) != uint(i) {
			t.Error("unexpected value")
		}
	}
}
