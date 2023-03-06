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

package edits

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/types"
)

func TestSortedEditItrStable(t *testing.T) {
	ctx := context.Background()
	vrw := types.NewMemoryValueStore()

	left := NewKVPCollection(vrw, types.KVPSlice{
		types.KVP{Key: types.Int(0)},
		types.KVP{Key: types.Int(1)},
		types.KVP{Key: types.Int(2)},
	})
	right := NewKVPCollection(vrw, types.KVPSlice{
		types.KVP{Key: types.Int(0), Val: types.Int(0)},
		types.KVP{Key: types.Int(1), Val: types.Int(0)},
		types.KVP{Key: types.Int(2), Val: types.Int(0)},
	})
	assert.NotNil(t, left)
	assert.NotNil(t, right)
	i := NewSortedEditItr(vrw, left, right)
	assert.NotNil(t, i)

	var err error
	var v *types.KVP
	v, err = i.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, types.Int(0), v.Key)
	assert.Nil(t, v.Val)
	v, err = i.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, types.Int(0), v.Key)
	assert.NotNil(t, v.Val)
	v, err = i.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, types.Int(1), v.Key)
	assert.Nil(t, v.Val)
	v, err = i.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, types.Int(1), v.Key)
	assert.NotNil(t, v.Val)
	v, err = i.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, types.Int(2), v.Key)
	assert.Nil(t, v.Val)
	v, err = i.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, types.Int(2), v.Key)
	assert.NotNil(t, v.Val)
	_, err = i.Next(ctx)
	assert.Equal(t, io.EOF, err)
}
