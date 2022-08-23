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

package spec

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func TestAbsolutePathToAndFromString(t *testing.T) {
	assert := assert.New(t)

	test := func(str string) {
		p, err := NewAbsolutePath(str)
		assert.NoError(err)
		assert.Equal(str, p.String())
	}

	h, err := types.Float(42).Hash(types.Format_Default) // arbitrary hash
	assert.NoError(err)
	test(fmt.Sprintf("/refs/heads/main"))
	test(fmt.Sprintf("#%s", h.String()))
}

func TestAbsolutePaths(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	cs := storage.NewView()
	vs := types.NewValueStore(cs)
	db := datas.NewTypesDatabase(vs, tree.NewNodeStore(cs))

	s0, s1 := types.String("foo"), types.String("bar")
	list, err := types.NewList(context.Background(), vs, s0, s1)
	assert.NoError(err)
	emptySet, err := types.NewSet(context.Background(), vs)
	assert.NoError(err)

	_, err = vs.WriteValue(context.Background(), s0)
	assert.NoError(err)
	_, err = vs.WriteValue(context.Background(), s1)
	assert.NoError(err)
	_, err = vs.WriteValue(context.Background(), list)
	assert.NoError(err)
	_, err = vs.WriteValue(context.Background(), emptySet)
	assert.NoError(err)

	ds, err := db.GetDataset(context.Background(), "ds")
	assert.NoError(err)
	ds, err = datas.CommitValue(context.Background(), db, ds, list)
	assert.NoError(err)
	head, hasHead := ds.MaybeHead()
	assert.True(hasHead)

	resolvesTo := func(exp types.Value, str string) {
		p, err := NewAbsolutePath(str)
		assert.NoError(err)
		act, err := p.Resolve(context.Background(), db, vs)
		assert.NoError(err)
		if exp == nil {
			assert.Nil(act)
		} else {
			assert.True(exp.Equals(act), "%s Expected %s Actual %s", str, mustString(types.EncodedValue(context.Background(), exp)), mustString(types.EncodedValue(context.Background(), act)))
		}
	}

	resolvesTo(head, "ds")
	resolvesTo(head, "#"+mustHash(head.Hash(vs.Format())).String())
	resolvesTo(list, "#"+mustHash(list.Hash(vs.Format())).String())
	resolvesTo(s0, "#"+mustHash(s0.Hash(vs.Format())).String())
	resolvesTo(s1, "#"+mustHash(s1.Hash(vs.Format())).String())

	resolvesTo(nil, "foo")
	resolvesTo(nil, "#"+mustHash(types.String("baz").Hash(vs.Format())).String())
	resolvesTo(nil, "#"+mustHash(types.String("baz").Hash(vs.Format())).String()+"[0]")
}

func TestAbsolutePathParseErrors(t *testing.T) {
	test := func(path, errMsg string) {
		p, err := NewAbsolutePath(path)
		assert.Equal(t, AbsolutePath{}, p)
		require.Error(t, err)
		assert.Equal(t, errMsg, err.Error())
	}

	test("", "empty path")
	test("#", "invalid hash: ")
	test("#abc", "invalid hash: abc")
	invHash := strings.Repeat("z", hash.StringLen)
	test("#"+invHash, "invalid hash: "+invHash)
}
