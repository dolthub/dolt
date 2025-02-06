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

package durable

import (
	"bytes"
	"context"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type proximityIndex struct {
	index prolly.ProximityMap
}

var _ Index = proximityIndex{}

// ProximityMapFromIndex unwraps the Index and returns the underlying prolly.ProximityMap.
func ProximityMapFromIndex(i Index) prolly.ProximityMap {
	return i.(proximityIndex).index
}

// IndexFromProximityMap wraps a prolly.ProximityMap and returns it as an Index.
func IndexFromProximityMap(m prolly.ProximityMap) Index {
	return proximityIndex{index: m}
}

// HashOf implements Index.
func (i proximityIndex) HashOf() (hash.Hash, error) {
	return i.index.HashOf(), nil
}

// Count implements Index.
func (i proximityIndex) Count() (uint64, error) {
	c, err := i.index.Count()
	return uint64(c), err
}

// Empty implements Index.
func (i proximityIndex) Empty() (bool, error) {
	c, err := i.index.Count()
	if err != nil {
		return false, err
	}
	return c == 0, nil
}

// Format implements Index.
func (i proximityIndex) Format() *types.NomsBinFormat {
	return types.Format_DOLT
}

// bytes implements Index.
func (i proximityIndex) bytes() ([]byte, error) {
	return shim.ValueFromMap(i.index).(types.SerialMessage), nil
}

var _ Index = proximityIndex{}

func (i proximityIndex) AddColumnToRows(ctx context.Context, newCol string, newSchema schema.Schema) (Index, error) {
	var last bool
	colIdx, iCol := 0, 0
	err := newSchema.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		last = false
		if strings.EqualFold(col.Name, newCol) {
			last = true
			colIdx = iCol
		}
		iCol++
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	_ = colIdx
	// If the column we added was last among non-primary key columns we can skip this step
	if last {
		return i, nil
	}

	// If not, then we have to iterate over this table's rows and update all the offsets for the new column
	rowMap := ProximityMapFromIndex(i)
	// TODO: Allow for mutation of ProximityMaps

	return IndexFromProximityMap(rowMap), nil
}

func (i proximityIndex) DebugString(ctx context.Context, ns tree.NodeStore, schema schema.Schema) string {
	var b bytes.Buffer
	i.index.WalkNodes(ctx, func(ctx context.Context, nd tree.Node) error {
		return tree.OutputProllyNode(ctx, &b, nd, ns, schema)
	})
	return b.String()
}
