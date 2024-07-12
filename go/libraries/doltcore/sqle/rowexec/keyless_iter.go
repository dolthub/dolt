package rowexec

import (
	"context"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// iterator that duplicates keyless rows by cardinality

// TODO tablescan
// TODO indexscan

func newKeylessMapIter(iter prolly.MapIter) prolly.MapIter {
	return &keylessSourceIter{iter: iter}
}

type keylessSourceIter struct {
	iter prolly.MapIter
	card uint64
	key  val.Tuple
	val  val.Tuple
}

var _ prolly.MapIter = (*keylessSourceIter)(nil)

func (k *keylessSourceIter) Next(ctx context.Context) (val.Tuple, val.Tuple, error) {
	var err error
	if k.key == nil {
		k.key, k.val, err = k.iter.Next(ctx)
		if err != nil {
			return nil, nil, err
		}
		k.card = val.ReadKeylessCardinality(k.val)
	}

	if k.card == 0 {
		k.key = nil
		k.val = nil
		return k.Next(ctx)
	}

	k.card--
	return k.key, k.val, nil
}
