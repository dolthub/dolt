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
	"bytes"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
)

type RefMapEdit struct {
	Name string
	Addr hash.Hash
}

func RefMapLookup(rm *serial.RefMap, key string) hash.Hash {
	var res hash.Hash
	n := sort.Search(rm.NamesLength(), func(i int) bool {
		return string(rm.Names(i)) >= key
	})
	if n != rm.NamesLength() && string(rm.Names(n)) == key {
		copy(res[:], rm.RefArrayBytes()[n*20:])
	}
	return res
}

func RefMapApplyEdits(rm *serial.RefMap, builder *flatbuffers.Builder, edits []RefMapEdit) flatbuffers.UOffsetT {
	sort.Slice(edits, func(i, j int) bool {
		return edits[i].Name < edits[j].Name
	})

	type idx struct {
		l, r int
	}
	var indexes []idx
	ni := 0
	ei := 0
	for ni < rm.NamesLength() && ei < len(edits) {
		if string(rm.Names(ni)) < edits[ei].Name {
			indexes = append(indexes, idx{ni, -1})
			ni += 1
		} else if string(rm.Names(ni)) == edits[ei].Name {
			if !edits[ei].Addr.IsEmpty() {
				indexes = append(indexes, idx{-1, ei})
			}
			ei += 1
			ni += 1
		} else {
			if !edits[ei].Addr.IsEmpty() {
				indexes = append(indexes, idx{-1, ei})
			}
			ei += 1
		}
	}
	for ni < rm.NamesLength() {
		indexes = append(indexes, idx{ni, -1})
		ni += 1
	}
	for ei < len(edits) {
		if !edits[ei].Addr.IsEmpty() {
			indexes = append(indexes, idx{-1, ei})
		}
		ei += 1
	}

	if len(indexes) == 0 {
		serial.RefMapStart(builder)
		return serial.RefMapEnd(builder)
	}

	nameoffs := make([]flatbuffers.UOffsetT, len(indexes))
	for i := len(nameoffs) - 1; i >= 0; i-- {
		var name string
		if indexes[i].l != -1 {
			name = string(rm.Names(indexes[i].l))
		} else {
			name = edits[indexes[i].r].Name
		}
		nameoffs[i] = builder.CreateString(name)
	}

	serial.RefMapStartNamesVector(builder, len(nameoffs))
	for i := len(nameoffs) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(nameoffs[i])
	}
	namesoff := builder.EndVector(len(nameoffs))

	hashsz := 20
	hashessz := len(indexes) * hashsz
	builder.Prep(flatbuffers.SizeUOffsetT, hashessz)
	stop := int(builder.Head())
	rmaddrbytes := rm.RefArrayBytes()
	start := stop - hashessz
	for _, idx := range indexes {
		if idx.l != -1 {
			copy(builder.Bytes[start:stop], rmaddrbytes[idx.l*20:idx.l*20+20])
		} else {
			copy(builder.Bytes[start:stop], edits[idx.r].Addr[:])
		}
		start += hashsz
	}
	start = stop - hashessz
	refarrayoff := builder.CreateByteVector(builder.Bytes[start:stop])

	serial.RefMapStart(builder)
	serial.RefMapAddNames(builder, namesoff)
	serial.RefMapAddRefArray(builder, refarrayoff)
	serial.RefMapAddTreeCount(builder, uint64(len(indexes)))
	serial.RefMapAddTreeLevel(builder, 0)
	return serial.RefMapEnd(builder)
}

type refmap struct {
	*serial.RefMap
}

func empty_refmap() refmap {
	builder := flatbuffers.NewBuilder(24)
	serial.RefMapStart(builder)
	builder.Finish(serial.RefMapEnd(builder))
	return refmap{serial.GetRootAsRefMap(builder.FinishedBytes(), 0)}
}

func (rm refmap) len() uint64 {
	return uint64(rm.RefMap.NamesLength())
}

func (rm refmap) edit(edits []RefMapEdit) refmap {
	builder := flatbuffers.NewBuilder(1024)
	builder.Finish(RefMapApplyEdits(rm.RefMap, builder, edits))
	return refmap{serial.GetRootAsRefMap(builder.FinishedBytes(), 0)}
}

func (rm refmap) lookup(key string) hash.Hash {
	return RefMapLookup(rm.RefMap, key)
}

func (rm refmap) set(key string, addr hash.Hash) refmap {
	return rm.edit([]RefMapEdit{{key, addr}})
}

func (rm *refmap) delete(key string) refmap {
	return rm.edit([]RefMapEdit{{key, hash.Hash{}}})
}

func (rm refmap) storeroot_flatbuffer() []byte {
	builder := flatbuffers.NewBuilder(1024)
	refmap := RefMapApplyEdits(rm.RefMap, builder, []RefMapEdit{})
	serial.StoreRootStart(builder)
	serial.StoreRootAddRefs(builder, refmap)
	builder.FinishWithFileIdentifier(serial.StoreRootEnd(builder), []byte(serial.StoreRootFileID))
	return builder.FinishedBytes()
}

func parse_storeroot(bs []byte) refmap {
	if !bytes.Equal([]byte(serial.StoreRootFileID), bs[4:8]) {
		panic("expected store root file id, got: " + string(bs[4:8]))
	}

	sr := serial.GetRootAsStoreRoot(bs, 0)
	rm := sr.Refs(nil)
	if rm == nil {
		panic("refmap of storeroot was missing")
	}
	if rm.TreeLevel() != 0 {
		panic("unsupported multi-level refmap")
	}
	if uint64(rm.NamesLength()) != rm.TreeCount() {
		panic("inconsistent refmap at level 0 where names length != tree count")
	}
	return refmap{rm}
}
