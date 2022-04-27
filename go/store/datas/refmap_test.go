// Copyright 2022 Dolthub, Inc.
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

package datas

import (
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestRefMapLookupEmpty(t *testing.T) {
	rm := empty_refmap().RefMap
	assert.Equal(t, rm.NamesLength(), 0)
	assert.Equal(t, RefMapLookup(rm, ""), hash.Hash{})
	assert.Equal(t, RefMapLookup(rm, "doesnotexist"), hash.Hash{})
}

func edit_refmap(rm *serial.RefMap, edits []RefMapEdit) *serial.RefMap {
	builder := flatbuffers.NewBuilder(1024)
	builder.Finish(RefMapApplyEdits(rm, builder, edits))
	return serial.GetRootAsRefMap(builder.FinishedBytes(), 0)
}

func TestRefMapEditInserts(t *testing.T) {
	empty := empty_refmap().RefMap
	a_hash := hash.Parse("3i50gcjrl9m2pgolrkc22kq46sj4p96o")
	with_a := edit_refmap(empty, []RefMapEdit{RefMapEdit{"a", a_hash}})
	assert.Equal(t, RefMapLookup(with_a, "a"), a_hash)
	assert.Equal(t, RefMapLookup(with_a, "A"), hash.Hash{})
	assert.Equal(t, RefMapLookup(empty, "a"), hash.Hash{})

	b_hash := hash.Parse("7mm15d7prjlurr8g4u51n7dfg6bemt7p")
	with_ab_from_a := edit_refmap(with_a, []RefMapEdit{RefMapEdit{"b", b_hash}})
	with_ab_from_empty := edit_refmap(empty, []RefMapEdit{RefMapEdit{"b", b_hash}, RefMapEdit{"a", a_hash}})
	assert.Equal(t, with_ab_from_a.Table().Bytes, with_ab_from_empty.Table().Bytes)
	assert.Equal(t, RefMapLookup(with_ab_from_a, "a"), a_hash)
	assert.Equal(t, RefMapLookup(with_ab_from_a, "b"), b_hash)
	assert.Equal(t, RefMapLookup(with_ab_from_a, "c"), hash.Hash{})
	assert.Equal(t, RefMapLookup(with_ab_from_a, "A"), hash.Hash{})
}

func TestRefMapEditDeletes(t *testing.T) {
	empty := empty_refmap().RefMap
	a_hash := hash.Parse("3i50gcjrl9m2pgolrkc22kq46sj4p96o")
	b_hash := hash.Parse("7mm15d7prjlurr8g4u51n7dfg6bemt7p")
	with_ab := edit_refmap(empty, []RefMapEdit{RefMapEdit{"b", b_hash}, RefMapEdit{"a", a_hash}})

	without_a := edit_refmap(with_ab, []RefMapEdit{{Name: "a"}})
	assert.Equal(t, RefMapLookup(without_a, "a"), hash.Hash{})
	assert.Equal(t, RefMapLookup(without_a, "b"), b_hash)

	without_ab := edit_refmap(without_a, []RefMapEdit{{Name: "b"}})
	assert.Equal(t, without_ab.NamesLength(), 0)
	assert.Equal(t, without_ab.RefArrayLength(), 0)
	assert.Equal(t, RefMapLookup(without_ab, "a"), hash.Hash{})
	assert.Equal(t, RefMapLookup(without_ab, "b"), hash.Hash{})
	assert.Equal(t, empty.Table().Bytes, without_ab.Table().Bytes)

	with_b := edit_refmap(empty, []RefMapEdit{RefMapEdit{"b", b_hash}})
	assert.Equal(t, without_a.Table().Bytes, with_b.Table().Bytes)

	delete_from_empty := edit_refmap(empty, []RefMapEdit{RefMapEdit{Name: "b"}})
	assert.Equal(t, delete_from_empty.Table().Bytes, empty.Table().Bytes)

}
