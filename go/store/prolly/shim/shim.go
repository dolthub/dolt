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
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func NodeFromValue(v types.Value) (node tree.Node, fileId string, err error) {
	return tree.NodeFromBytes(v.(types.SerialMessage))
}

func ValueFromMap(m prolly.MapInterface) types.Value {
	return tree.ValueFromNode(m.Node())
}

func MapFromValue(v types.Value, sch schema.Schema, ns tree.NodeStore, isKeylessSecondary bool) (prolly.Map, error) {
	root, fileId, err := NodeFromValue(v)
	if fileId == serial.VectorIndexNodeFileID {
		return prolly.Map{}, fmt.Errorf("can't make a prolly.Map from a vector index node")
	}
	if err != nil {
		return prolly.Map{}, err
	}
	kd := sch.GetKeyDescriptor()
	if isKeylessSecondary {
		kd = prolly.AddHashToSchema(kd)
	}
	vd := sch.GetValueDescriptor()
	return prolly.NewMap(root, ns, kd, vd), nil
}

func MapInterfaceFromValue(ctx context.Context, v types.Value, sch schema.Schema, ns tree.NodeStore, isKeylessSecondary bool) (prolly.MapInterface, error) {
	root, fileId, err := NodeFromValue(v)
	if err != nil {
		return nil, err
	}
	kd := sch.GetKeyDescriptor()
	if isKeylessSecondary {
		kd = prolly.AddHashToSchema(kd)
	}
	vd := sch.GetValueDescriptor()
	switch fileId {
	case serial.VectorIndexNodeFileID:
		// TODO: We should read the distance function and chunk size from the message.
		// Currently, vector.DistanceL2Squared{} and prolly.DefaultLogChunkSize are the only values that can be written,
		// but this may not be true in the future.
		return prolly.NewProximityMap(ctx, ns, root, kd, vd, vector.DistanceL2Squared{}, prolly.DefaultLogChunkSize), nil
	default:
		return prolly.NewMap(root, ns, kd, vd), nil
	}
}

func MapFromValueWithDescriptors(v types.Value, kd, vd val.TupleDesc, ns tree.NodeStore) (prolly.MapInterface, error) {
	root, fileId, err := NodeFromValue(v)
	if err != nil {
		return prolly.Map{}, err
	}
	switch fileId {
	case serial.VectorIndexNodeFileID:
		// TODO: We should read the distance function and chunk size from the message.
		// Currently, vector.DistanceL2Squared{} and prolly.DefaultLogChunkSize are the only values that can be written,
		// but this may not be true in the future.
		return prolly.NewProximityMap(nil, ns, root, kd, vd, vector.DistanceL2Squared{}, prolly.DefaultLogChunkSize), nil
	default:
		return prolly.NewMap(root, ns, kd, vd), nil
	}
}
