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

func TestWorkingSetMergeStateTableSchemas(t *testing.T) {
	working := hash.Of([]byte("working"))
	preMerge := hash.Of([]byte("pre-merge working"))
	from := hash.Of([]byte("from"))
	preMergeHead := hash.Of([]byte("pre-merge head"))
	names := []string{"s1.t", "t", "t"}
	for _, test := range []struct {
		name    string
		schemas []string
		want    []string
	}{
		{"legacy names", nil, nil},
		{"default schemas", []string{"", "", ""}, nil},
		{"named schemas", []string{"", "s1", "s2"}, []string{"", "s1", "s2"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			mergeState := &MergeState{
				preMergeWorkingAddr: &preMerge, fromCommitAddr: &from, preMergeHeadCommitAddr: &preMergeHead,
				fromCommitSpec: "source", unmergableTables: names, unmergableTableSchemas: test.schemas,
				isRevert: true, pendingRevertHashes: []string{"next"},
			}
			msg := workingset_flatbuffer(working, &working, mergeState, nil, nil)
			head, err := newSerialWorkingSetHead(msg, hash.Of(msg))
			require.NoError(t, err)
			stored, err := head.msg.TryMergeState(nil)
			require.NoError(t, err)
			require.Equal(t, len(test.want), stored.UnmergableTableSchemasLength())
			if test.want == nil {
				// Default-schema writes retain the original eight-field format,
				// so older readers can still read them.
				require.LessOrEqual(t, int(stored.Table().NumFields()), 8)
			}
			decoded, err := head.HeadWorkingSet()
			require.NoError(t, err)
			expected := *mergeState
			expected.unmergableTableSchemas = test.want
			require.Equal(t, &expected, decoded.MergeState)
			require.Equal(t, test.want, decoded.MergeState.UnmergableTableSchemas())
		})
	}

	for _, schemas := range [][]string{{"s1"}, {"s1", "", "s2", "extra"}} {
		t.Run("mismatched schema count", func(t *testing.T) {
			_, err := NewMergeState(types.Ref{}, nil, "source", names, schemas, false, false, nil, nil)
			require.ErrorContains(t, err, "table schemas for 3 unmergable tables")
			mergeState := &MergeState{
				preMergeWorkingAddr: &preMerge, fromCommitAddr: &from,
				unmergableTables: names, unmergableTableSchemas: schemas,
			}
			msg := workingset_flatbuffer(working, &working, mergeState, nil, nil)
			head, err := newSerialWorkingSetHead(msg, hash.Of(msg))
			require.NoError(t, err)
			_, err = head.HeadWorkingSet()
			require.ErrorContains(t, err, "table schemas for 3 unmergable tables")
		})
	}
}

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
