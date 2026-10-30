// Copyright 2026 Dolthub, Inc.
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

package datas

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func TestWorkingSetWalkAddrs(t *testing.T) {
	working := hash.Of([]byte("working"))
	staged := hash.Of([]byte("staged"))
	preRebase := hash.Of([]byte("pre-rebase working"))
	onto := hash.Of([]byte("onto"))

	walk := func(t *testing.T, msg types.SerialMessage) []hash.Hash {
		var addrs []hash.Hash
		require.NoError(t, msg.WalkAddrs(types.Format_DOLT, func(addr hash.Hash) error {
			addrs = append(addrs, addr)
			return nil
		}))
		return addrs
	}

	t.Run("rebase state", func(t *testing.T) {
		rebaseState := NewRebaseState(preRebase, onto, "feat", 0, 0, 0, false, false)
		msg := types.SerialMessage(workingset_flatbuffer(working, &staged, nil, rebaseState, nil))
		assert.ElementsMatch(t, []hash.Hash{working, staged, preRebase, onto}, walk(t, msg))
	})

	t.Run("merge state", func(t *testing.T) {
		preMerge := hash.Of([]byte("pre-merge working"))
		from := hash.Of([]byte("from"))
		preMergeHead := hash.Of([]byte("pre-merge head"))
		mergeState := &MergeState{preMergeWorkingAddr: &preMerge, fromCommitAddr: &from, preMergeHeadCommitAddr: &preMergeHead}
		msg := types.SerialMessage(workingset_flatbuffer(working, &staged, mergeState, nil, nil))
		assert.ElementsMatch(t, []hash.Hash{working, staged, preMerge, from, preMergeHead}, walk(t, msg))
	})

	t.Run("merge state without a pre-merge head", func(t *testing.T) {
		preMerge := hash.Of([]byte("pre-merge working"))
		from := hash.Of([]byte("from"))
		mergeState := &MergeState{preMergeWorkingAddr: &preMerge, fromCommitAddr: &from}
		msg := types.SerialMessage(workingset_flatbuffer(working, &staged, mergeState, nil, nil))
		assert.ElementsMatch(t, []hash.Hash{working, staged, preMerge, from}, walk(t, msg))
	})

	t.Run("no rebase state", func(t *testing.T) {
		msg := types.SerialMessage(workingset_flatbuffer(working, &staged, nil, nil, nil))
		assert.ElementsMatch(t, []hash.Hash{working, staged}, walk(t, msg))
	})
}
