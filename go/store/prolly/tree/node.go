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

package tree

import (
	"context"
	"encoding/hex"
	"io"
	"math"

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/types"
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

type Item []byte

type Node struct {
	keys, values val.SlicedBuffer
	buf          serial.TupleMap
	count        uint16
}

type AddressCb func(ctx context.Context, addr hash.Hash) error

func WalkAddresses(ctx context.Context, nd Node, ns NodeStore, cb AddressCb) error {
	if nd.IsLeaf() {
		// todo(andy): ref'd values
		return nil
	}

	for i := 0; i < int(nd.count); i++ {
		addr := nd.getRef(i)

		if err := cb(ctx, addr); err != nil {
			return err
		}

		child, err := ns.Read(ctx, addr)
		if err != nil {
			return err
		}

		if err := WalkAddresses(ctx, child, ns, cb); err != nil {
			return err
		}
	}
	return nil
}

type NodeCb func(ctx context.Context, nd Node) error

func WalkNodes(ctx context.Context, nd Node, ns NodeStore, cb NodeCb) error {
	if err := cb(ctx, nd); err != nil {
		return err
	}
	if nd.IsLeaf() {
		// todo(andy): walk ref'd values?
		return nil
	}

	for i := 0; i < int(nd.count); i++ {
		child, err := ns.Read(ctx, nd.getRef(i))
		if err != nil {
			return err
		}
		if err := WalkNodes(ctx, child, ns, cb); err != nil {
			return err
		}
	}
	return nil
}

func NewEmptyNode(pool pool.BuffPool) Node {
	bld := &nodeBuilder{}
	return bld.build(pool)
}

func NodeFromBytes(bb []byte) Node {
	buf := serial.GetRootAsTupleMap(bb, 0)
	return nodeFromFlatbuffer(*buf)
}

func nodeFromFlatbuffer(buf serial.TupleMap) Node {
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

func (nd Node) HashOf() hash.Hash {
	return hash.Of(nd.bytes())
}

func (nd Node) Count() int {
	return int(nd.count)
}

func (nd Node) TreeCount() int {
	return int(nd.buf.TreeCount())
}

func (nd Node) Size() int {
	return len(nd.bytes())
}

// Level returns the tree Level for this node
func (nd Node) Level() int {
	return int(nd.buf.TreeLevel())
}

// IsLeaf returns whether this node is a leaf
func (nd Node) IsLeaf() bool {
	return int(nd.buf.TreeLevel()) == 0
}

// GetKey returns the |ith| key of this node
func (nd Node) GetKey(i int) Item {
	return nd.keys.GetSlice(i)
}

// getValue returns the |ith| value of this node. Only Valid for leaf nodes.
func (nd Node) getValue(i int) Item {
	if nd.IsLeaf() {
		return nd.values.GetSlice(i)
	} else {
		r := nd.getRef(i)
		return r[:]
	}
}

// getRef returns the |ith| ref in this node. Only Valid for internal nodes.
func (nd Node) getRef(i int) hash.Hash {
	refs := nd.buf.RefArrayBytes()
	start, stop := i*refSize, (i+1)*refSize
	return hash.New(refs[start:stop])
}

func (nd Node) getSubtreeCounts() subtreeCounts {
	buf := nd.buf.RefCardinalitiesBytes()
	return readSubtreeCounts(int(nd.count), buf)
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

// OutputProllyNode writes the node given to the writer given in a semi-human-readable format, where values are still
// displayed in hex-encoded byte strings, but are delineated into their fields. All nodes have keys displayed in this
// manner. Interior nodes have their child hash references spelled out, leaf nodes have value tuples delineated like
// the keys
func OutputProllyNode(w io.Writer, node Node) error {
	w.Write([]byte("["))
	for i := 0; i < int(node.count); i++ {
		k := node.GetKey(i)
		kt := val.Tuple(k)

		w.Write([]byte("\n    { key: "))
		for j := 0; j < kt.Count(); j++ {
			if j > 0 {
				w.Write([]byte(", "))
			}
			w.Write([]byte(hex.EncodeToString(kt.GetField(j))))
		}

		if node.IsLeaf() {
			v := node.getValue(i)
			vt := val.Tuple(v)

			w.Write([]byte(" value: "))
			for j := 0; j < vt.Count(); j++ {
				if j > 0 {
					w.Write([]byte(", "))
				}
				w.Write([]byte(hex.EncodeToString(vt.GetField(j))))
			}

			w.Write([]byte(" }"))
		} else {
			ref := node.getRef(i)

			w.Write([]byte(" ref: #"))
			w.Write([]byte(ref.String()))
			w.Write([]byte(" }"))
		}
	}

	w.Write([]byte("\n]\n"))
	return nil
}

func ValueFromNode(root Node) types.Value {
	return types.TupleRowStorage(root.bytes())
}
