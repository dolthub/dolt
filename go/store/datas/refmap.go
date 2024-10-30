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
	"fmt"
	flatbuffers "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func storeroot_flatbuffer(am prolly.AddressMap) serial.Message {
	builder := flatbuffers.NewBuilder(1024)
	ambytes := []byte(tree.ValueFromNode(am.Node()).(types.SerialMessage))
	voff := builder.CreateByteVector(ambytes)
	serial.StoreRootStart(builder)
	serial.StoreRootAddAddressMap(builder, voff)
	return serial.FinishMessage(builder, serial.StoreRootEnd(builder), []byte(serial.StoreRootFileID))
}

func parse_storeroot(bs []byte, ns tree.NodeStore) (prolly.AddressMap, error) {
	if serial.GetFileID(bs) != serial.StoreRootFileID {
		panic("expected store root file id, got: " + serial.GetFileID(bs))
	}
	sr, err := serial.TryGetRootAsStoreRoot(bs, serial.MessagePrefixSz)
	if err != nil {
		return prolly.AddressMap{}, err
	}
	mapbytes := sr.AddressMapBytes()
	node, fileId, err := tree.NodeFromBytes(mapbytes)
	if err != nil {
		return prolly.AddressMap{}, err
	}
	if fileId != serial.AddressMapFileID {
		return prolly.AddressMap{}, fmt.Errorf("unexpected file ID for address map, expected %s, found %s", serial.AddressMapFileID, fileId)
	}
	return prolly.NewAddressMap(node, ns)
}
