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

	ctx context.Context
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

func (q *q) AddPendingIfUnseen(id hash.Hash) error {
	c, err := q.Get(id)
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

func (q *q) SetInvisible(id hash.Hash) error {
	c, err := q.Get(id)
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

func (q *q) load(h hash.Hash) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(h.String(), "")
	if err != nil {
		return nil, err
	}
	c, err := q.ddb.Resolve(q.ctx, cs)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (q *q) Get(id hash.Hash) (*c, error) {
	if l, ok := q.loaded[id]; ok {
		return l, nil
	} else {
		l, err := q.load(id)
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

func newQueue(ctx context.Context, ddb *doltdb.DoltDB) *q {
	return &q{ctx: ctx, ddb: ddb, loaded: make(map[hash.Hash]*c)}
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
	q := newQueue(ctx, ddb)
	if err := q.SetInvisible(excludedHead); err != nil {
		return nil, err
	}
	if err := q.AddPendingIfUnseen(excludedHead); err != nil {
		return nil, err
	}
	if err := q.AddPendingIfUnseen(includedHead); err != nil {
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
				if err := q.SetInvisible(parentID); err != nil {
					return nil, err
				}
			}
			if err := q.AddPendingIfUnseen(parentID); err != nil {
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
