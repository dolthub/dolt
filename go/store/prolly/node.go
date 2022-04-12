// Copyright 2021 Dolthub, Inc.
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

package prolly

import (
	"math"

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	maxVectorOffset = uint64(math.MaxUint16)
	refSize         = hash.ByteLen

	// These constants are mirrored from serial.TupleMap.KeyOffsetsLength()
	// and serial.TupleMap.ValueOffsetsLength() respectively.
	// They are only as stable as the flatbuffers schemas that define them.
	keyOffsetsVOffset   = 6
	valueOffsetsVOffset = 10
)

type Node struct {
	keys, values val.SlicedBuffer
	buf          serial.TupleMap
	count        uint16
}

func mapNodeFromBytes(bb []byte) Node {
	buf := serial.GetRootAsTupleMap(bb, 0)
	return mapNodeFromFlatbuffer(*buf)
}

func mapNodeFromFlatbuffer(buf serial.TupleMap) Node {
	keys := val.SlicedBuffer{
		Buf:  buf.KeyTuplesBytes(),
		Offs: getKeyOffsetsVector(buf),
	}
	values := val.SlicedBuffer{
		Buf:  buf.ValueTuplesBytes(),
		Offs: getValueOffsetsVector(buf),
	}

	count := buf.KeyOffsetsLength() + 1
	if len(keys.Buf) == 0 {
		count = 0
	}

	return Node{
		keys:   keys,
		values: values,
		count:  uint16(count),
		buf:    buf,
	}
}

func (nd Node) hashOf() hash.Hash {
	return hash.Of(nd.bytes())
}

func (nd Node) GetKey(i int) nodeItem {
	return nd.keys.GetSlice(i)
}

func (nd Node) GetValue(i int) nodeItem {
	if nd.leafNode() {
		return nd.values.GetSlice(i)
	} else {
		r := nd.getRef(i)
		return r[:]
	}
}

func (nd Node) Size() int {
	return nd.keys.Len()
}

func (nd Node) getRef(i int) hash.Hash {
	refs := nd.buf.RefArrayBytes()
	start, stop := i*refSize, (i+1)*refSize
	return hash.New(refs[start:stop])
}

func (nd Node) treeCount() int {
	return int(nd.buf.TreeCount())
}

func (nd Node) getSubtreeCounts() subtreeCounts {
	buf := nd.buf.RefCardinalitiesBytes()
	return readSubtreeCounts(int(nd.count), buf)
}

func (nd Node) level() int {
	return int(nd.buf.TreeLevel())
}

func (nd Node) leafNode() bool {
	return nd.level() == 0
}

func (nd Node) empty() bool {
	return nd.bytes() == nil || nd.count == 0
}

func (nd Node) bytes() []byte {
	return nd.buf.Table().Bytes
}

func getKeyOffsetsVector(buf serial.TupleMap) []byte {
	sz := buf.KeyOffsetsLength() * 2
	tab := buf.Table()
	vec := tab.Offset(keyOffsetsVOffset)
	start := int(tab.Vector(fb.UOffsetT(vec)))
	stop := start + sz

	return tab.Bytes[start:stop]
}

func getValueOffsetsVector(buf serial.TupleMap) []byte {
	sz := buf.ValueOffsetsLength() * 2
	tab := buf.Table()
	vec := tab.Offset(valueOffsetsVOffset)
	start := int(tab.Vector(fb.UOffsetT(vec)))
	stop := start + sz

	return tab.Bytes[start:stop]
}
