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

// testMap is a utility type that allows us to create a common test
// harness for Map, memoryMap, and MutableMap.
type testMap interface {
	Get(ctx context.Context, key val.Tuple, cb KeyValueFn[val.Tuple, val.Tuple]) (err error)
	IterAll(ctx context.Context) (MapIter, error)
	IterRange(ctx context.Context, rng Range) (MapIter, error)
}

var _ testMap = Map{}
var _ testMap = MutableMap{}

type ordinalMap interface {
	testMap
	IterOrdinalRange(ctx context.Context, start, stop uint64) (MapIter, error)
}

var _ testMap = Map{}

func countOrderedMap(t *testing.T, om testMap) (cnt int) {
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

func keyDescFromMap(om testMap) val.TupleDesc {
	switch m := om.(type) {
	case Map:
		return m.keyDesc
	case MutableMap:
		return m.keyDesc
	default:
		panic("unknown ordered map")
	}
}
