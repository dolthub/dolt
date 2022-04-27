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
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

// orderedMap is a utility type that allows us to create a common test
// harness for Map, memoryMap, and MutableMap.
type orderedMap interface {
	Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error)
	IterAll(ctx context.Context) (MapRangeIter, error)
	IterRange(ctx context.Context, rng Range) (MapRangeIter, error)
}

var _ orderedMap = Map{}
var _ orderedMap = MutableMap{}
var _ orderedMap = memoryMap{}

type ordinalMap interface {
	orderedMap
	IterOrdinalRange(ctx context.Context, start, stop uint64) (MapRangeIter, error)
}

var _ orderedMap = Map{}

func countOrderedMap(t *testing.T, om orderedMap) (cnt int) {
	iter, err := om.IterAll(context.Background())
	require.NoError(t, err)
	for {
		_, _, err = iter.Next(context.Background())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		cnt++
	}
	return cnt
}

func keyDescFromMap(om orderedMap) val.TupleDesc {
	switch m := om.(type) {
	case Map:
		return m.keyDesc
	case MutableMap:
		return m.prolly.keyDesc
	case memoryMap:
		return m.keyDesc
	default:
		panic("unknown ordered map")
	}
}
