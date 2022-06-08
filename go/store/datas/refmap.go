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

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func storeroot_flatbuffer(am prolly.AddressMap) []byte {
	builder := flatbuffers.NewBuilder(1024)
	ambytes := []byte(tree.ValueFromNode(am.Node()).(types.TupleRowStorage))
	voff := builder.CreateByteVector(ambytes)
	serial.StoreRootStart(builder)
	serial.StoreRootAddAddressMap(builder, voff)
	builder.FinishWithFileIdentifier(serial.StoreRootEnd(builder), []byte(serial.StoreRootFileID))
	return builder.FinishedBytes()
}

func parse_storeroot(bs []byte, cs chunks.ChunkStore) prolly.AddressMap {
	if !bytes.Equal([]byte(serial.StoreRootFileID), bs[4:8]) {
		panic("expected store root file id, got: " + string(bs[4:8]))
	}
	sr := serial.GetRootAsStoreRoot(bs, 0)
	mapbytes := sr.AddressMapBytes()
	node := tree.NodeFromBytes(mapbytes)
	return prolly.NewAddressMap(node, tree.NewNodeStore(cs))
}
