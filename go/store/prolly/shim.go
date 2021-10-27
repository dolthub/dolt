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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func NewEmptyMap(sch schema.Schema) Map {
	return Map{
		root:    emptyNode,
		keyDesc: keyDescriptorFromSchema(sch),
		valDesc: valueDescriptorFromSchema(sch),
	}
}

func ValueFromNode(nd Node) types.Value {
	return types.InlineBlob(nd)
}

func NodeFromValue(v types.Value) Node {
	return Node(v.(types.InlineBlob))
}

func ValueFromMap(m Map) types.Value {
	return types.InlineBlob(m.root)
}

func MapFromValue(v types.Value, sch schema.Schema, vrw types.ValueReadWriter) Map {
	return Map{
		root:    NodeFromValue(v),
		keyDesc: keyDescriptorFromSchema(sch),
		valDesc: valueDescriptorFromSchema(sch),
		nrw:     NewNodeStore(vrw.(*types.ValueStore).ChunkStore()),
	}
}

func keyDescriptorFromSchema(sch schema.Schema) (kd val.TupleDesc) {
	// todo(andy)
	return kd
}

func valueDescriptorFromSchema(sch schema.Schema) (vd val.TupleDesc) {
	// todo(andy)
	return vd
}
