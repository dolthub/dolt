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

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type Item []byte

type Node struct {
	// keys and values contain sub-slices of |msg|,
	// allowing faster lookups by avoiding the vtable
	keys, values val.SlicedBuffer
	count        uint16
	msg          message.Message
}

type AddressCb func(ctx context.Context, addr hash.Hash) error

func WalkAddresses(ctx context.Context, nd Node, ns NodeStore, cb AddressCb) error {
	return walkAddresses(ctx, nd, func(ctx context.Context, addr hash.Hash) error {
		if err := cb(ctx, addr); err != nil {
			return err
		}

		child, err := ns.Read(ctx, addr)
		if err != nil {
			return err
		}

		return WalkAddresses(ctx, child, ns, cb)
	})
}

type NodeCb func(ctx context.Context, nd Node) error

func WalkNodes(ctx context.Context, nd Node, ns NodeStore, cb NodeCb) error {
	if err := cb(ctx, nd); err != nil {
		return err
	}

	return walkAddresses(ctx, nd, func(ctx context.Context, addr hash.Hash) error {
		child, err := ns.Read(ctx, addr)
		if err != nil {
			return err
		}
		return WalkNodes(ctx, child, ns, cb)
	})
}

func NodeFromBytes(msg []byte) Node {
	keys, values, count := message.GetKeysAndValues(msg)
	return Node{
		keys:   keys,
		values: values,
		count:  count,
		msg:    msg,
	}
}

func (nd Node) HashOf() hash.Hash {
	return hash.Of(nd.bytes())
}

func (nd Node) Count() int {
	return int(nd.count)
}

func (nd Node) TreeCount() int {
	return message.GetTreeCount(nd.msg)
}

func (nd Node) Size() int {
	return len(nd.bytes())
}

// Level returns the tree Level for this node
func (nd Node) Level() int {
	return message.GetTreeLevel(nd.msg)
}

// IsLeaf returns whether this node is a leaf
func (nd Node) IsLeaf() bool {
	return nd.Level() == 0
}

// GetKey returns the |ith| key of this node
func (nd Node) GetKey(i int) Item {
	return nd.keys.GetSlice(i)
}

// getValue returns the |ith| value of this node.
func (nd Node) getValue(i int) Item {
	return nd.values.GetSlice(i)
}

// getAddress returns the |ith| address of this node.
// This method assumes values are 20-byte address hashes.
func (nd Node) getAddress(i int) hash.Hash {
	return hash.New(nd.getValue(i))
}

func (nd Node) getSubtreeCounts() SubtreeCounts {
	return message.GetSubtrees(nd.msg)
}

func (nd Node) empty() bool {
	return nd.bytes() == nil || nd.count == 0
}

func (nd Node) bytes() []byte {
	return nd.msg
}

func walkAddresses(ctx context.Context, nd Node, cb AddressCb) (err error) {
	return message.WalkAddresses(ctx, nd.msg, cb)
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
			ref := node.getAddress(i)

			w.Write([]byte(" ref: #"))
			w.Write([]byte(ref.String()))
			w.Write([]byte(" }"))
		}
	}

	w.Write([]byte("\n]\n"))
	return nil
}

func OutputAddressMapNode(w io.Writer, node Node) error {
	w.Write([]byte("["))
	for i := 0; i < int(node.count); i++ {
		k := node.GetKey(i)
		w.Write([]byte("\n    { key: "))
		w.Write(k)

		ref := node.getAddress(i)

		w.Write([]byte(" ref: #"))
		w.Write([]byte(ref.String()))
		w.Write([]byte(" }"))
	}
	w.Write([]byte("\n]\n"))
	return nil
}

func ValueFromNode(root Node) types.Value {
	return types.TupleRowStorage(root.bytes())
}
