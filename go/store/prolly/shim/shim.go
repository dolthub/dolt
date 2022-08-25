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

package shim

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func NodeFromValue(v types.Value) (tree.Node, error) {
	return tree.NodeFromBytes(v.(types.SerialMessage))
}

func ValueFromMap(m prolly.Map) types.Value {
	return tree.ValueFromNode(m.Node())
}

func ValueFromArtifactMap(m prolly.ArtifactMap) types.Value {
	return tree.ValueFromNode(m.Node())
}

func MapFromValue(v types.Value, sch schema.Schema, ns tree.NodeStore) (prolly.Map, error) {
	root, err := NodeFromValue(v)
	if err != nil {
		return prolly.Map{}, err
	}
	kd := sch.GetKeyDescriptor()
	vd := sch.GetValueDescriptor()
	return prolly.NewMap(root, ns, kd, vd), nil
}
