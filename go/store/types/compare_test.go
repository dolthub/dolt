// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareTotalOrdering(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	// values in increasing order. Some of these are compared by ref so changing the serialization might change the ordering.
	values := []Value{
		Bool(false), Bool(true),
		Float(-10), Float(0), Float(10),
		String("a"), String("b"), String("c"),

		// The order of these are done by the hash.
		mustValue(NewSet(context.Background(), vrw, Float(0), Float(1), Float(2), Float(3))),
		PrimitiveTypeMap[BoolKind],

		// Value - values cannot be value
		// Cycle - values cannot be cycle
		// Union - values cannot be unions
	}

	ctx := context.Background()

	for i, vi := range values {
		for j, vj := range values {
			if i == j {
				assert.True(vi.Equals(vj))
			} else if i < j {
				x, err := vi.Less(ctx, vrw.Format(), vj)
				require.NoError(t, err)
				assert.True(x)
			} else {
				x, err := vi.Less(ctx, vrw.Format(), vj)
				require.NoError(t, err)
				assert.False(x)
			}
		}
	}
}

func encode(nbf *NomsBinFormat, v Value) []byte {
	w := &binaryNomsWriter{make([]byte, 128), 0}
	v.writeTo(w, nbf)
	return w.data()
}

func compareInts(i, j int) (res int) {
	if i < j {
		res = -1
	} else if i > j {
		res = 1
	}
	return
}
