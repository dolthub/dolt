// Copyright 2022 Dolthub, Inc.
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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func TestToposort(t *testing.T) {
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewViewWithDefaultFormat()).(*database)
	a, b, c, d := "ds-a", "ds-b", "ds-c", "ds-d"
	a1 := addCommit2(t, db, a, "a1")
	d1 := addCommit2(t, db, d, "d1")
	a2 := addCommit2(t, db, a, "a2", a1)
	c2 := addCommit2(t, db, c, "c2", a1)
	d2 := addCommit2(t, db, d, "d2", d1)
	a3 := addCommit2(t, db, a, "a3", a2)
	b3 := addCommit2(t, db, b, "b3", a2)
	c3 := addCommit2(t, db, c, "c3", c2, d2)
	a4 := addCommit2(t, db, a, "a4", a3)
	b4 := addCommit2(t, db, b, "b4", b3)
	a5 := addCommit2(t, db, a, "a5", a4)
	b5 := addCommit2(t, db, b, "b5", b4, a3)
	a6 := addCommit2(t, db, a, "a6", a5, b5)
	// ds-a: a1<-a2<-a3<-a4<-a5<-a6
	//       ^    ^   ^          |
	//       |     \   \----\  /-/
	//       |      \        \V
	// ds-b:  \      b3<-b4<-b5
	//         \
	//          \
	// ds-c:     c2<-c3
	//              /
	//             /
	//            V
	// ds-d: d1<-d2
	tests := []struct {
		head *Commit
		exp  []string
	}{
		{
			head: b5,
			exp:  []string{"a1", "a2", "b3", "b4", "a3", "b5"},
		},
		{
			head: a6,
			exp:  []string{"a1", "a2", "a3", "a4", "a5", "b3", "b4", "b5", "a6"},
		},
		{
			head: c3,
			exp:  []string{"a1", "c2", "d1", "d2", "c3"},
		},
		{
			head: a4,
			exp:  []string{"a1", "a2", "a3", "a4"},
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.exp), func(t *testing.T) {
			s := &topoState{
				vr:   db,
				out:  make([]*Commit, 0),
				seen: make(map[hash.Hash]*Commit),
			}
			topoSort(context.Background(), s, tt.head, -1)
			var cmp []string
			for i := range s.out {
				rootV, err := GetCommittedValue(context.Background(), db, s.out[i].val)
				require.NoError(t, err)
				cmp = append(cmp, string(rootV.(types.String)))
			}
			fmt.Println(cmp)
			require.Equal(t, tt.exp, cmp)
		})
	}
}

// Add a commit and return it.
func addCommit2(t *testing.T, db *database, datasetID string, val string, parents ...*Commit) *Commit {
	ds, err := db.GetDataset(context.Background(), datasetID)
	require.NoError(t, err)
	ds, err = db.Commit(context.Background(), ds, types.String(val), CommitOptions{Parents: mustCommitToTargetHashes2(db, parents...)})
	require.NoError(t, err)
	return &Commit{mustHead(ds), mustHeadAddr(ds), mustHeight(ds)}
}

func mustCommitToTargetHashes2(vrw types.ValueReadWriter, commits ...*Commit) []hash.Hash {
	ret := make([]hash.Hash, len(commits))
	for i, c := range commits {
		r, err := types.NewRef(c.val, vrw.Format())
		if err != nil {
			panic(err)
		}
		ret[i] = r.TargetHash()
	}
	return ret
}
