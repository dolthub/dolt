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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

func TestAddressMap(t *testing.T) {
	t.Run("smoke test address map", func(t *testing.T) {
		ctx := context.Background()
		ns := tree.NewTestNodeStore()
		addr, err := ns.Write(ctx, tree.NewEmptyTestNode())
		require.NoError(t, err)
		pairs := randomAddressPairs(10_000, addr)

		empty, err := NewEmptyAddressMap(ns)
		require.NoError(t, err)
		editor := empty.Editor()

		for _, p := range pairs {
			err = editor.Add(ctx, p.name(), p.addr())
			assert.NoError(t, err)
		}
		am, err := editor.Flush(ctx)
		assert.NoError(t, err)

		rand.Shuffle(len(pairs), func(i, j int) {
			pairs[i], pairs[j] = pairs[j], pairs[i]
		})

		for _, p := range pairs {
			var act hash.Hash
			act, err = am.Get(ctx, p.name())
			assert.NoError(t, err)
			assert.Equal(t, p.addr(), act)
		}
	})
}

type addrPair struct {
	n []byte
	h hash.Hash
}

func (a addrPair) name() string {
	return string(a.n)
}

func (a addrPair) addr() hash.Hash {
	return a.h
}

func randomAddressPairs(cnt int, addr hash.Hash) (ap []addrPair) {
	buf := make([]byte, cnt*20*2)
	testRand.Read(buf)

	ap = make([]addrPair, cnt)
	for i := range ap {
		o := i * 40
		ap[i].n = buf[o : o+20]
		ap[i].h = addr
	}
	return
}
