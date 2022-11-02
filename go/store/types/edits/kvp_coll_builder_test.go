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

package edits

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/types"
)

func TestAddKVP(t *testing.T) {
	nbf := types.Format_Default
	builder := NewKVPCollBuilder(2, nbf)
	builder.AddKVP(types.KVP{Key: types.Uint(0), Val: types.NullValue})
	builder.AddKVP(types.KVP{Key: types.Uint(1), Val: types.NullValue})
	builder.AddKVP(types.KVP{Key: types.Uint(2), Val: types.NullValue})

	coll := builder.Build()
	itr := coll.Iterator()

	for i := int64(0); i < coll.Size(); i++ {
		kvp, err := itr.Next()
		assert.NoError(t, err)

		if uint(kvp.Key.(types.Uint)) != uint(i) {
			t.Error("enexpected value")
		}
	}
}

func TestMoveRemaining(t *testing.T) {
	nbf := types.Format_Default
	sl1 := types.KVPSlice{{Key: types.Uint(0), Val: types.NullValue}, {Key: types.Uint(1), Val: types.NullValue}}
	sl2 := types.KVPSlice{{Key: types.Uint(2), Val: types.NullValue}, {}}
	coll := &KVPCollection{
		2,
		2,
		3,
		[]types.KVPSlice{sl1, sl2[:1]},
		nbf,
	}

	builder := NewKVPCollBuilder(2, nbf)
	builder.MoveRemaining(coll.Iterator())

	result := builder.Build()
	itr := result.Iterator()

	for i := int64(0); i < result.Size(); i++ {
		kvp, err := itr.Next()
		assert.NoError(t, err)

		if uint(kvp.Key.(types.Uint)) != uint(i) {
			t.Error("enexpected value")
		}
	}
}

func TestAddKVPAndMoveRemaining(t *testing.T) {
	nbf := types.Format_Default
	sl := types.KVPSlice{{Key: types.Uint(1), Val: types.NullValue}, {Key: types.Uint(2), Val: types.NullValue}}
	coll := NewKVPCollection(nbf, sl)

	builder := NewKVPCollBuilder(2, nbf)
	builder.AddKVP(types.KVP{Key: types.Uint(0), Val: types.NullValue})
	builder.MoveRemaining(coll.Iterator())

	result := builder.Build()
	itr := result.Iterator()

	for i := int64(0); i < result.Size(); i++ {
		kvp, err := itr.Next()
		assert.NoError(t, err)

		if uint(kvp.Key.(types.Uint)) != uint(i) {
			t.Error("unexpected value")
		}
	}
}
