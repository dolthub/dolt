// Copyright 2019 Liquidata, Inc.
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

package commitwalk

import (
	"context"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

type c struct {
	commit    *doltdb.Commit
	height    uint64
	invisible bool
	queued    bool
}

type q struct {
	pending           []*c
	numVisiblePending int
	loaded            map[hash.Hash]*c

	ddb *doltdb.DoltDB
}

func (q *q) NumVisiblePending() int {
	return q.numVisiblePending
}

func (q *q) PopPending() *c {
	c := q.pending[len(q.pending)-1]
	q.pending = q.pending[:len(q.pending)-1]
	if !c.invisible {
		q.numVisiblePending--
	}
	return c
}

func (q *q) AddPendingIfUnseen(ctx context.Context, id hash.Hash) error {
	c, err := q.Get(ctx, id)
	if err != nil {
		return err
	}
	if !c.queued {
		c.queued = true
		var i int
		for i = 0; i < len(q.pending); i++ {
			if q.pending[i].height > c.height {
				break
			}
		}
		q.pending = append(q.pending, nil)
		copy(q.pending[i+1:], q.pending[i:])
		q.pending[i] = c
		if !c.invisible {
			q.numVisiblePending++
		}
	}
	return nil
}

func (q *q) SetInvisible(ctx context.Context, id hash.Hash) error {
	c, err := q.Get(ctx, id)
	if err != nil {
		return err
	}
	if !c.invisible {
		c.invisible = true
		if c.queued {
			q.numVisiblePending--
		}
	}
	return nil
}

func (q *q) load(ctx context.Context, h hash.Hash) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(h.String(), "")
	if err != nil {
		return nil, err
	}
	c, err := q.ddb.Resolve(ctx, cs)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (q *q) Get(ctx context.Context, id hash.Hash) (*c, error) {
	if l, ok := q.loaded[id]; ok {
		return l, nil
	} else {
		l, err := q.load(ctx, id)
		if err != nil {
			return nil, err
		}
		h, err := l.Height()
		if err != nil {
			return nil, err
		}
		c := &c{commit: l, height: h}
		q.loaded[id] = c
		return c, nil
	}
}

func newQueue(ddb *doltdb.DoltDB) *q {
	return &q{ddb: ddb, loaded: make(map[hash.Hash]*c)}
}

// Return the commits reachable from commit at hash `includedHead`
// that are not reachable from hash `excludedHead`. `includedHead` and
// `excludedHead` must be commits in `ddb`. Returns up to `num`
// commits, in reverse topological order starting at `includedHead`,
// with tie breaking based on the height of commit graph between
// concurrent commits --- higher commits appear first. Beyond the
// deterministic tie-break, concurrent commits are ordered
// non-deterministically.
//
// Roughly mimics `git log master..feature`.
func GetDotDotRevisions(ctx context.Context, ddb *doltdb.DoltDB, includedHead hash.Hash, excludedHead hash.Hash, num int) ([]*doltdb.Commit, error) {
	commitList := make([]*doltdb.Commit, 0, num)
	q := newQueue(ddb)
	if err := q.SetInvisible(ctx, excludedHead); err != nil {
		return nil, err
	}
	if err := q.AddPendingIfUnseen(ctx, excludedHead); err != nil {
		return nil, err
	}
	if err := q.AddPendingIfUnseen(ctx, includedHead); err != nil {
		return nil, err
	}
	for q.NumVisiblePending() > 0 {
		nextC := q.PopPending()
		parents, err := nextC.commit.ParentHashes(ctx)
		if err != nil {
			return nil, err
		}
		for _, parentID := range parents {
			if nextC.invisible {
				if err := q.SetInvisible(ctx, parentID); err != nil {
					return nil, err
				}
			}
			if err := q.AddPendingIfUnseen(ctx, parentID); err != nil {
				return nil, err
			}
		}
		if !nextC.invisible {
			commitList = append(commitList, nextC.commit)
			if len(commitList) == num {
				return commitList, nil
			}
		}
	}
	return commitList, nil
}
