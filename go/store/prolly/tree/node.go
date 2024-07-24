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
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type Item []byte

type subtreeCounts []uint64

// Node is a generic implementation of a prolly tree node.
// Elements in a Node are generic Items. Interpreting Item
// contents is deferred to higher layers (see prolly.Map).
type Node struct {
	// keys and values cache offset metadata
	// to accelerate Item lookups into msg.
	keys, values message.ItemAccess

	// count is the Item pair count.
	count uint16

	// level is 0-indexed tree height.
	level uint16

	// subtrees contains the key cardinality
	// of each child tree of a non-leaf Node.
	// this field is lazily decoded from msg
	// because it requires a malloc.
	subtrees *subtreeCounts

	// msg is the underlying buffer for the Node
	// encoded as a Flatbuffers message.
	msg serial.Message
}

type AddressCb func(ctx context.Context, addr hash.Hash) error

func WalkAddresses(ctx context.Context, nd Node, ns NodeStore, cb AddressCb) error {
	return walkAddresses(ctx, nd, func(ctx context.Context, addr hash.Hash) error {
		if err := cb(ctx, addr); err != nil {
			return err
		}

		if nd.IsLeaf() {
			return nil
		}

		child, err := ns.Read(ctx, addr)
		if err != nil {
			return err
		}

		return WalkAddresses(ctx, child, ns, cb)
	})
}

type NodeCb func(ctx context.Context, nd Node) error

// WalkNodes runs a callback function on every node found in the DFS of |nd|
// that is of the same message type as |nd|.
func WalkNodes(ctx context.Context, nd Node, ns NodeStore, cb NodeCb) error {
	if err := cb(ctx, nd); err != nil {
		return err
	}
	if nd.IsLeaf() {
		return nil
	}

	return walkAddresses(ctx, nd, func(ctx context.Context, addr hash.Hash) error {
		child, err := ns.Read(ctx, addr)
		if err != nil {
			return err
		}
		return WalkNodes(ctx, child, ns, cb)
	})
}

// walkOpaqueNodes runs a callback function on every node found in the DFS of |nd|
// including nested trees.
func walkOpaqueNodes(ctx context.Context, nd Node, ns NodeStore, cb NodeCb) error {
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

func NodeFromBytes(msg []byte) (Node, error) {
	keys, values, level, count, err := message.UnpackFields(msg)
	return Node{
		keys:   keys,
		values: values,
		count:  count,
		level:  level,
		msg:    msg,
	}, err
}

func (nd Node) HashOf() hash.Hash {
	return hash.Of(nd.bytes())
}

func (nd Node) Count() int {
	return int(nd.count)
}

func (nd Node) TreeCount() (int, error) {
	return message.GetTreeCount(nd.msg)
}

func (nd Node) Size() int {
	return len(nd.bytes())
}

// Level returns the tree Level for this node
func (nd Node) Level() int {
	return int(nd.level)
}

// IsLeaf returns whether this node is a leaf
func (nd Node) IsLeaf() bool {
	return nd.level == 0
}

// GetKey returns the |ith| key of this node
func (nd Node) GetKey(i int) Item {
	return nd.keys.GetItem(i, nd.msg)
}

// GetValue returns the |ith| value of this node.
func (nd Node) GetValue(i int) Item {
	return nd.values.GetItem(i, nd.msg)
}

func (nd Node) loadSubtrees() (Node, error) {
	var err error
	if nd.subtrees == nil {
		// deserializing subtree counts requires a malloc,
		// we don't load them unless explicitly requested
		sc, err := message.GetSubtrees(nd.msg)
		if err != nil {
			return Node{}, err
		}
		nd.subtrees = (*subtreeCounts)(&sc)
	}
	return nd, err
}

func (nd Node) getSubtreeCount(i int) (uint64, error) {
	if nd.IsLeaf() {
		return 1, nil
	}
	// this will panic unless subtrees were loaded.
	return (*nd.subtrees)[i], nil
}

// getAddress returns the |ith| address of this node.
// This method assumes values are 20-byte address hashes.
func (nd Node) getAddress(i int) hash.Hash {
	return hash.New(nd.GetValue(i))
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

func getLastKey(nd Node) Item {
	return nd.GetKey(int(nd.count) - 1)
}

// OutputProllyNode writes the node given to the writer given in a human-readable format, with values converted
// to the type specified by the provided schema. All nodes have keys displayed in this manner. Interior nodes have
// their child hash references spelled out, leaf nodes have value tuples delineated like the keys
func OutputProllyNode(ctx context.Context, w io.Writer, node Node, ns NodeStore, schema schema.Schema) error {
	kd := schema.GetKeyDescriptor()
	vd := schema.GetValueDescriptor()
	for i := 0; i < int(node.count); i++ {
		k := node.GetKey(i)
		kt := val.Tuple(k)

		w.Write([]byte("\n    { key: "))
		for j := 0; j < kt.Count(); j++ {
			if j > 0 {
				w.Write([]byte(", "))
			}

			isAddr := val.IsAddrEncoding(kd.Types[j].Enc)
			if isAddr {
				w.Write([]byte("#"))
			}
			w.Write([]byte(hex.EncodeToString(kd.GetField(j, kt))))
			if isAddr {
				w.Write([]byte(" ("))
				key, err := GetField(ctx, kd, j, kt, ns)
				if err != nil {
					return err
				}
				w.Write([]byte(fmt.Sprint(key)))
				w.Write([]byte(")"))
			}

		}

		if node.IsLeaf() {
			v := node.GetValue(i)
			vt := val.Tuple(v)

			w.Write([]byte(" value: "))
			for j := 0; j < vt.Count(); j++ {
				if j > 0 {
					w.Write([]byte(", "))
				}
				isAddr := val.IsAddrEncoding(vd.Types[j].Enc)
				if isAddr {
					w.Write([]byte("#"))
				}
				w.Write([]byte(hex.EncodeToString(vd.GetField(j, vt))))
				if isAddr {
					w.Write([]byte(" ("))
					value, err := GetField(ctx, vd, j, vt, ns)
					if err != nil {
						return err
					}
					w.Write([]byte(fmt.Sprint(value)))
					w.Write([]byte(")"))
				}
			}

			w.Write([]byte(" }"))
		} else {
			ref := node.getAddress(i)

			w.Write([]byte(" ref: #"))
			w.Write([]byte(ref.String()))
			w.Write([]byte(" }"))
		}
	}

	w.Write([]byte("\n"))
	return nil
}

// OutputProllyNodeBytes writes the node given to the writer given in a semi-human-readable format, where values are still
// displayed in hex-encoded byte strings, but are delineated into their fields. All nodes have keys displayed in this
// manner. Interior nodes have their child hash references spelled out, leaf nodes have value tuples delineated like
// the keys
func OutputProllyNodeBytes(w io.Writer, node Node) error {
	return types.OutputProllyNodeBytes(w, node.msg)
}

func OutputAddressMapNode(w io.Writer, node Node) error {
	for i := 0; i < int(node.count); i++ {
		k := node.GetKey(i)
		w.Write([]byte("\n    { key: "))
		w.Write(k)

		ref := node.getAddress(i)

		w.Write([]byte(" ref: #"))
		w.Write([]byte(ref.String()))
		w.Write([]byte(" }"))
	}
	w.Write([]byte("\n"))
	return nil
}

func ValueFromNode(root Node) types.Value {
	return types.SerialMessage(root.bytes())
}
