// Copyright 2024 Dolthub, Inc.
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

package util

import (
	"context"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func OutputEncodedValue(ctx context.Context, w io.Writer, value types.Value) error {
	switch value := value.(type) {
	// Some types of serial message need to be output here because of dependency cycles between types / tree package
	case types.SerialMessage:
		switch serial.GetFileID(value) {
		case serial.TableFileID:
			msg, err := serial.TryGetRootAsTable(value, serial.MessagePrefixSz)
			if err != nil {
				return err
			}

			fmt.Fprintf(w, " {\n")
			fmt.Fprintf(w, "\tSchema: #%s\n", hash.New(msg.SchemaBytes()).String())
			fmt.Fprintf(w, "\tViolations: #%s\n", hash.New(msg.ViolationsBytes()).String())
			fmt.Fprintf(w, "\tArtifacts: #%s\n", hash.New(msg.ArtifactsBytes()).String())
			// TODO: merge conflicts, not stable yet

			fmt.Fprintf(w, "\tAutoinc: %d\n", msg.AutoIncrementValue())

			// clustered index
			node, err := tree.NodeFromBytes(msg.PrimaryIndexBytes())
			if err != nil {
				return err
			}
			c, err := node.TreeCount()
			if err != nil {
				return err
			}
			fmt.Fprintf(w, "\tPrimary Index (rows %d, depth %d) #%s {",
				c, node.Level()+1, node.HashOf().String())
			tree.OutputProllyNodeBytes(w, node)
			fmt.Fprintf(w, "\t}\n")

			// secondary indexes
			node, err = tree.NodeFromBytes(msg.SecondaryIndexesBytes())
			if err != nil {
				return err
			}
			c, err = node.TreeCount()
			if err != nil {
				return err
			}
			fmt.Fprintf(w, "\tSecondary Indexes (indexes %d, depth %d) %s {",
				c, node.Level()+1, node.HashOf().String()[:8])
			err = tree.OutputAddressMapNode(w, node)
			if err != nil {
				return err
			}
			fmt.Fprintf(w, "\t}\n")
			fmt.Fprintf(w, "}")

			return nil
		case serial.StoreRootFileID:
			msg, err := serial.TryGetRootAsStoreRoot(value, serial.MessagePrefixSz)
			if err != nil {
				return err
			}
			ambytes := msg.AddressMapBytes()
			node, err := tree.NodeFromBytes(ambytes)
			if err != nil {
				return err
			}
			return tree.OutputAddressMapNode(w, node)
		case serial.ProllyTreeNodeFileID:
			fallthrough
		case serial.AddressMapFileID:
			node, err := shim.NodeFromValue(value)
			if err != nil {
				return err
			}
			return tree.OutputProllyNodeBytes(w, node)
		default:
			return types.WriteEncodedValue(ctx, w, value)
		}
	default:
		return types.WriteEncodedValue(ctx, w, value)
	}
}
