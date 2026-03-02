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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

func TestWalkRefs(t *testing.T) {
	runTest := func(nbf *NomsBinFormat, v Value, t *testing.T) {
		assert := assert.New(t)
		expected := hash.HashSlice{}
		v.walkRefs(nbf, func(r Ref) error {
			expected = append(expected, r.TargetHash())
			return nil
		})
		val, err := EncodeValue(v, nbf)
		require.NoError(t, err)
		err = walkRefs(val.Data(), nbf, func(r Ref) error {
			if assert.True(len(expected) > 0) {
				assert.Equal(expected[0], r.TargetHash())
				expected = expected[1:]
			}

			return nil
		})
		require.NoError(t, err)
		assert.Len(expected, 0)
	}

	t.Run("SingleRef", func(t *testing.T) {
		t.Parallel()
		t.Run("OfValue", func(t *testing.T) {
			runTest(Format_Default, mustValue(ToRefOfValue(mustRef(NewRef(Bool(false), Format_Default)), Format_Default)), t)
		})
	})

	t.Run("Struct", func(t *testing.T) {
		t.Parallel()
		data := StructData{
			"ref": mustRef(NewRef(Bool(false), Format_Default)),
			"num": Float(42),
		}
		st, err := NewStruct(Format_Default, "nom", data)
		require.NoError(t, err)
		runTest(Format_Default, st, t)
	})
}
